// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The connection cache for one configured Fluss cluster.
//!
//! Connections are indexed by effective Fluss user and multiplexed by `fluss-rs`. Service identity
//! mode is the one-entry case: `ConnectionCache::key` always returns the configured service user.
//! Concurrent cold requests for the same user serialize behind one dial. A cancelled request cancels
//! its dial, releases the dial lock, and lets the next waiter retry; dial failures are not cached.
//!
//! Idle cleanup is based on the last acquisition time rather than a lease count. Configuration keeps
//! the idle timeout above the longest backend operation, so no request can still be using a connection
//! once it becomes eligible. Shutdown similarly relies on the process lifecycle draining request and
//! cleaner tasks before calling `ConnectionCache::close`.

use crate::backend::context::RequestContext;
use crate::backend::errors::map_fluss_error;
use crate::backend::types::ClusterId;
use crate::config::{ClusterConfig, ConnectionSecurityProtocol, IdentityMode};
use crate::error::{GatewayError, GatewayResult};
use crate::observability;
use fluss::client::FlussConnection;
use fluss::config::Config as NativeClientConfig;
use fluss::error::Error as FlussClientError;
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use tokio::time::Instant;

/// How often the process-owned cleaner scans every cluster cache.
pub(crate) const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Per-connection budget for an idle close. Idle cleanup is best effort and must not stall later scans.
const CLEANER_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Private cache key for a service connection without Fluss authentication.
///
/// A configured SASL service account and every future request-derived principal are non-blank, so this
/// sentinel cannot collide with an authenticated effective user.
const PLAINTEXT_SERVICE_USER: &str = "";

/// The two Fluss-side users of one authenticated native connection.
///
/// `gateway_user` authenticates the connection. `fluss_user` is the effective user Fluss authorizes.
/// They are the same configured service account today. Future user identity mode keeps the Gateway
/// account in the first field and maps the request [`crate::backend::context::Principal`] into the
/// second immediately before selecting a connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FlussConnectionIdentity {
    gateway_user: Arc<str>,
    fluss_user: Arc<str>,
}

impl FlussConnectionIdentity {
    fn service(config: &ClusterConfig) -> Option<Self> {
        if config.security_protocol == ConnectionSecurityProtocol::Plaintext {
            return None;
        }
        let service_user: Arc<str> = Arc::from(
            config
                .service_account()
                .expect("validated SASL configuration has a service account"),
        );
        Some(Self {
            gateway_user: service_user.clone(),
            fluss_user: service_user,
        })
    }

    fn gateway_user(&self) -> &str {
        &self.gateway_user
    }

    fn fluss_user(&self) -> &str {
        &self.fluss_user
    }
}

/// Opens and releases connections for one cluster.
///
/// The cache owns sharing and cleanup; this trait owns what a connection is, which lets those
/// algorithms be tested without a cluster.
pub(crate) trait Connector: Send + Sync + 'static {
    type Conn: Send + Sync + 'static;

    fn dial(
        &self,
        identity: Option<&FlussConnectionIdentity>,
    ) -> impl Future<Output = Result<Self::Conn, FlussClientError>> + Send;

    /// Releases a connection, draining pending writes within `timeout`.
    fn close(
        &self,
        connection: Arc<Self::Conn>,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), FlussClientError>> + Send;
}

/// Dials the native Fluss client with the cluster's configured security protocol.
pub(crate) struct NativeConnector {
    config: NativeClientConfig,
}

impl NativeConnector {
    pub(crate) fn new(config: &ClusterConfig) -> Self {
        Self {
            config: config.native_client_config(),
        }
    }
}

impl Connector for NativeConnector {
    type Conn = FlussConnection;

    async fn dial(
        &self,
        identity: Option<&FlussConnectionIdentity>,
    ) -> Result<FlussConnection, FlussClientError> {
        if let Some(identity) = identity {
            if identity.gateway_user() != identity.fluss_user() {
                return Err(FlussClientError::UnsupportedOperation {
                    message: "user identity mode requires SASL authorization ID support"
                        .to_string(),
                });
            }
            debug_assert_eq!(self.config.security_sasl_username, identity.gateway_user());
        } else {
            debug_assert!(!self.config.is_sasl_enabled());
        }
        FlussConnection::new(self.config.clone()).await
    }

    async fn close(
        &self,
        connection: Arc<FlussConnection>,
        timeout: Duration,
    ) -> Result<(), FlussClientError> {
        connection.close(timeout).await
    }
}

/// One effective user's cache entry.
#[derive(Debug)]
struct CachedConnection<C> {
    /// Installed once for this entry and never replaced. Eviction removes the entire entry.
    connection: OnceLock<Arc<C>>,
    /// Serializes cold dials for this user without blocking dials for other users.
    dialing: tokio::sync::Mutex<()>,
    /// Milliseconds relative to [`ConnectionCache::origin`], updated whenever a request acquires the
    /// entry. `fetch_max` prevents an older concurrent reader from moving the timestamp backwards.
    last_access_ms: AtomicU64,
}

impl<C> CachedConnection<C> {
    fn new(now_ms: u64) -> Self {
        Self {
            connection: OnceLock::new(),
            dialing: tokio::sync::Mutex::new(()),
            last_access_ms: AtomicU64::new(now_ms),
        }
    }

    fn touch(&self, now_ms: u64) {
        self.last_access_ms.fetch_max(now_ms, Ordering::Relaxed);
    }
}

struct CacheState<C> {
    closed: bool,
    entries: HashMap<Arc<str>, Arc<CachedConnection<C>>>,
}

/// The connections of one configured Fluss cluster, indexed by effective Fluss user.
pub(crate) struct ConnectionCache<K: Connector> {
    cluster: ClusterId,
    /// Service-mode cache key: the service account under SASL, or a private sentinel for plaintext.
    gateway_user: Arc<str>,
    identity: Option<FlussConnectionIdentity>,
    connector: K,
    idle_timeout: Duration,
    origin: Instant,
    state: RwLock<CacheState<K::Conn>>,
}

impl<K: Connector> ConnectionCache<K> {
    pub(crate) fn new(cluster: ClusterId, config: &ClusterConfig, connector: K) -> Self {
        assert_eq!(
            config.identity_mode,
            IdentityMode::Service,
            "validated configuration must reject user identity mode"
        );
        let identity = FlussConnectionIdentity::service(config);
        let gateway_user = identity
            .as_ref()
            .map(|identity| identity.fluss_user.clone())
            .unwrap_or_else(|| Arc::from(PLAINTEXT_SERVICE_USER));
        Self {
            cluster,
            gateway_user,
            identity,
            connector,
            idle_timeout: config.connection_idle_timeout.get(),
            origin: Instant::now(),
            state: RwLock::new(CacheState {
                closed: false,
                entries: HashMap::new(),
            }),
        }
    }

    /// Returns the shared connection for the request's effective Fluss user.
    ///
    /// The dial belongs to the calling request. Cancelling the request drops the dial future and its
    /// mutex guard; the next waiter then retries. A native dial failure is likewise not cached.
    pub(crate) async fn connection(&self, ctx: &RequestContext) -> GatewayResult<Arc<K::Conn>> {
        let key = self.key(ctx);
        if let Some(connection) = self.hit(&key)? {
            return Ok(connection);
        }
        self.dial_for(&key).await
    }

    /// Removes and closes connections whose last acquisition is beyond the configured idle timeout.
    pub(crate) async fn clean_expired(&self) {
        let connections = {
            let mut state = self.write_state();
            if state.closed {
                return;
            }
            self.take_expired(&mut state)
        };
        if connections.is_empty() {
            return;
        }

        let results = join_all(
            connections
                .iter()
                .cloned()
                .map(|connection| self.connector.close(connection, CLEANER_CLOSE_TIMEOUT)),
        )
        .await;
        for result in results {
            if let Err(error) = result {
                log::warn!(
                    "failed to drain an idle Fluss connection of cluster `{}`: {error}",
                    self.cluster
                );
            }
            observability::connection_closed(self.cluster.as_str(), "idle");
        }
        observability::connections_active(self.cluster.as_str(), self.live_count());
    }

    /// Closes and removes every cached connection. Idempotent.
    ///
    /// The process lifecycle must drain every request and stop the cleaner before calling this method.
    /// Consequently no dial or operation can race with the map drain; `closed` only prevents accidental
    /// requests after that lifecycle boundary from opening a connection the process would not own.
    pub(crate) async fn close(&self, timeout: Duration) -> GatewayResult<()> {
        let connections = {
            let mut state = self.write_state();
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            state
                .entries
                .drain()
                .filter_map(|(_, entry)| entry.connection.get().cloned())
                .collect::<Vec<_>>()
        };

        let closes = connections.len();
        let results = join_all(
            connections
                .into_iter()
                .map(|connection| self.connector.close(connection, timeout)),
        )
        .await;
        let mut first_failure = None;
        for result in results {
            if let Err(error) = result {
                first_failure = first_failure
                    .or_else(|| Some(map_fluss_error("close the Fluss connection", error, None)));
            }
            observability::connection_closed(self.cluster.as_str(), "shutdown");
        }
        if closes > 0 {
            observability::connections_active(self.cluster.as_str(), 0);
        }
        first_failure.map_or(Ok(()), Err)
    }

    async fn dial_for(&self, key: &Arc<str>) -> GatewayResult<Arc<K::Conn>> {
        let entry = self.admit(key)?;
        // TODO: Share one failed dial with its current waiters without caching it for later requests;
        // see #4101.
        let _dialing = entry.dialing.lock().await;

        // The caller that won the previous dial may have installed the connection while this request
        // waited for the user-local mutex.
        if let Some(connection) = entry.connection.get() {
            entry.touch(self.elapsed_ms());
            return Ok(connection.clone());
        }

        let connection = self
            .connector
            .dial(self.identity.as_ref())
            .await
            .map_err(|native| map_fluss_error("connect to Fluss", native, None))?;
        Ok(self.install(&entry, connection))
    }

    fn install(&self, entry: &Arc<CachedConnection<K::Conn>>, connection: K::Conn) -> Arc<K::Conn> {
        let connection = Arc::new(connection);
        let installed = entry.connection.set(connection.clone());
        assert!(
            installed.is_ok(),
            "a user-local dial mutex must allow only one connection installation"
        );
        entry.touch(self.elapsed_ms());
        observability::connection_created(self.cluster.as_str());
        observability::connections_active(self.cluster.as_str(), self.live_count());
        if let Some(identity) = &self.identity {
            log::info!(
                "connected to Fluss cluster `{}` as Fluss user `{}` through Gateway user `{}`",
                self.cluster,
                identity.fluss_user(),
                identity.gateway_user()
            );
        } else {
            log::info!(
                "connected to Fluss cluster `{}` without Fluss authentication",
                self.cluster
            );
        }
        connection
    }

    fn hit(&self, key: &str) -> GatewayResult<Option<Arc<K::Conn>>> {
        let state = self.read_state();
        if state.closed {
            return Err(GatewayError::unavailable(
                "the Fluss connection cache is closed",
            ));
        }
        let Some(entry) = state.entries.get(key) else {
            return Ok(None);
        };
        let Some(connection) = entry.connection.get() else {
            return Ok(None);
        };
        entry.touch(self.elapsed_ms());
        Ok(Some(connection.clone()))
    }

    fn admit(&self, key: &Arc<str>) -> GatewayResult<Arc<CachedConnection<K::Conn>>> {
        {
            let state = self.read_state();
            if state.closed {
                return Err(GatewayError::unavailable(
                    "the Fluss connection cache is closed",
                ));
            }
            if let Some(entry) = state.entries.get(key) {
                // Refresh under the state lock so the cleaner cannot remove an entry being redialed.
                entry.touch(self.elapsed_ms());
                return Ok(entry.clone());
            }
        }

        let mut state = self.write_state();
        if state.closed {
            return Err(GatewayError::unavailable(
                "the Fluss connection cache is closed",
            ));
        }
        if let Some(entry) = state.entries.get(key) {
            entry.touch(self.elapsed_ms());
            return Ok(entry.clone());
        }
        let entry = Arc::new(CachedConnection::new(self.elapsed_ms()));
        state.entries.insert(key.clone(), entry.clone());
        Ok(entry)
    }

    /// The only identity-mode branch point. Service mode always has exactly one key; future user mode
    /// maps the request principal into an effective Fluss user here.
    fn key(&self, _ctx: &RequestContext) -> Arc<str> {
        self.gateway_user.clone()
    }

    fn take_expired(&self, state: &mut CacheState<K::Conn>) -> Vec<Arc<K::Conn>> {
        let now_ms = self.elapsed_ms();
        let threshold_ms = u64::try_from(self.idle_timeout.as_millis()).unwrap_or(u64::MAX);
        let mut expired = Vec::new();
        state.entries.retain(|_, entry| {
            if now_ms.saturating_sub(entry.last_access_ms.load(Ordering::Relaxed)) < threshold_ms {
                return true;
            }
            if let Some(connection) = entry.connection.get() {
                expired.push(connection.clone());
            }
            false
        });
        expired
    }

    fn live_count(&self) -> usize {
        self.read_state()
            .entries
            .values()
            .filter(|entry| entry.connection.get().is_some())
            .count()
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn read_state(&self) -> RwLockReadGuard<'_, CacheState<K::Conn>> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, CacheState<K::Conn>> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::errors::tests::api_failure;
    use crate::config::ConfigDuration;
    use crate::error::ErrorKind;
    use fluss::error::FlussError;
    use futures_util::future::join_all;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FakeConnection;

    #[derive(Clone)]
    enum Outcome {
        Connect,
        Fail(FlussError),
        Hang,
    }

    struct Dialer {
        dials: Mutex<Vec<Option<FlussConnectionIdentity>>>,
        closes: AtomicUsize,
        outcome: Mutex<Outcome>,
        latency: Duration,
        active_dials: AtomicUsize,
        max_active_dials: AtomicUsize,
    }

    impl Dialer {
        fn new(outcome: Outcome, latency: Duration) -> Arc<Self> {
            Arc::new(Self {
                dials: Mutex::new(Vec::new()),
                closes: AtomicUsize::new(0),
                outcome: Mutex::new(outcome),
                latency,
                active_dials: AtomicUsize::new(0),
                max_active_dials: AtomicUsize::new(0),
            })
        }

        fn connecting() -> Arc<Self> {
            Self::new(Outcome::Connect, Duration::ZERO)
        }

        fn set(&self, outcome: Outcome) {
            *self.outcome.lock().unwrap() = outcome;
        }

        fn dialed(&self) -> Vec<Option<FlussConnectionIdentity>> {
            self.dials.lock().unwrap().clone()
        }

        fn closes(&self) -> usize {
            self.closes.load(Ordering::SeqCst)
        }

        fn max_active_dials(&self) -> usize {
            self.max_active_dials.load(Ordering::SeqCst)
        }
    }

    struct ActiveDial(Arc<Dialer>);

    impl Drop for ActiveDial {
        fn drop(&mut self) {
            self.0.active_dials.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl Connector for Arc<Dialer> {
        type Conn = FakeConnection;

        fn dial(
            &self,
            identity: Option<&FlussConnectionIdentity>,
        ) -> impl Future<Output = Result<FakeConnection, FlussClientError>> + Send {
            let dialer = self.clone();
            let identity = identity.cloned();
            async move {
                dialer.dials.lock().unwrap().push(identity);
                let active = dialer.active_dials.fetch_add(1, Ordering::SeqCst) + 1;
                dialer.max_active_dials.fetch_max(active, Ordering::SeqCst);
                let _active = ActiveDial(dialer.clone());
                let outcome = dialer.outcome.lock().unwrap().clone();
                if !dialer.latency.is_zero() {
                    tokio::time::sleep(dialer.latency).await;
                }
                match outcome {
                    Outcome::Connect => Ok(FakeConnection),
                    Outcome::Fail(error) => Err(api_failure(error)),
                    Outcome::Hang => std::future::pending().await,
                }
            }
        }

        fn close(
            &self,
            _connection: Arc<FakeConnection>,
            _timeout: Duration,
        ) -> impl Future<Output = Result<(), FlussClientError>> + Send {
            self.closes.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(()))
        }
    }

    fn cache(dialer: &Arc<Dialer>, config: ClusterConfig) -> ConnectionCache<Arc<Dialer>> {
        ConnectionCache::new(
            ClusterId::try_from("default").unwrap(),
            &config,
            dialer.clone(),
        )
    }

    fn service_cache(dialer: &Arc<Dialer>) -> ConnectionCache<Arc<Dialer>> {
        cache(dialer, service_config())
    }

    fn service_config() -> ClusterConfig {
        ClusterConfig {
            security_protocol: ConnectionSecurityProtocol::Sasl,
            service_account: Some("gateway_svc".to_string()),
            service_secret: Some(crate::config::Secret::new("secret")),
            ..ClusterConfig::default()
        }
    }

    fn context() -> RequestContext {
        RequestContext::for_test("default", Duration::from_secs(5))
    }

    #[tokio::test]
    async fn concurrent_first_requests_share_a_single_dial() {
        let dialer = Dialer::new(Outcome::Connect, Duration::from_millis(50));
        let cache = Arc::new(service_cache(&dialer));

        let connections = join_all((0..32).map(|_| {
            let cache = cache.clone();
            async move { cache.connection(&context()).await.unwrap() }
        }))
        .await;

        assert_eq!(dialer.dialed().len(), 1);
        assert_eq!(dialer.max_active_dials(), 1);
        assert!(
            connections
                .windows(2)
                .all(|pair| Arc::ptr_eq(&pair[0], &pair[1])),
            "every caller must share the connection"
        );
        assert_eq!(cache.live_count(), 1);
    }

    #[tokio::test]
    async fn concurrent_failures_are_serialized_but_not_cached() {
        let dialer = Dialer::new(
            Outcome::Fail(FlussError::NetworkException),
            Duration::from_millis(10),
        );
        let cache = Arc::new(service_cache(&dialer));

        let failures = join_all((0..8).map(|_| {
            let cache = cache.clone();
            async move { cache.connection(&context()).await.unwrap_err() }
        }))
        .await;

        assert!(
            failures
                .iter()
                .all(|failure| failure.kind() == ErrorKind::Unavailable)
        );
        assert_eq!(dialer.dialed().len(), 8);
        assert_eq!(dialer.max_active_dials(), 1);
        assert_eq!(cache.live_count(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_caller_releases_the_dial_for_the_next_request() {
        let dialer = Dialer::new(Outcome::Hang, Duration::ZERO);
        let cache = service_cache(&dialer);

        let cancelled =
            tokio::time::timeout(Duration::from_millis(20), cache.connection(&context())).await;
        assert!(cancelled.is_err());

        dialer.set(Outcome::Connect);
        assert!(cache.connection(&context()).await.is_ok());
        assert_eq!(dialer.dialed().len(), 2);
        assert_eq!(dialer.max_active_dials(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_failed_entry_is_not_removed_while_redialing() {
        let dialer = Dialer::new(Outcome::Fail(FlussError::NetworkException), Duration::ZERO);
        let cache = Arc::new(cache(
            &dialer,
            ClusterConfig {
                connection_idle_timeout: ConfigDuration::from_secs(2),
                ..service_config()
            },
        ));

        assert!(cache.connection(&context()).await.is_err());
        tokio::time::advance(Duration::from_secs(2)).await;

        dialer.set(Outcome::Hang);
        let retry_cache = cache.clone();
        let retry = tokio::spawn(async move { retry_cache.connection(&context()).await });
        while dialer.dialed().len() < 2 {
            tokio::task::yield_now().await;
        }

        cache.clean_expired().await;
        assert_eq!(cache.read_state().entries.len(), 1);

        let follower_cache = cache.clone();
        let follower = tokio::spawn(async move { follower_cache.connection(&context()).await });
        tokio::task::yield_now().await;
        assert_eq!(dialer.dialed().len(), 2);
        assert_eq!(dialer.max_active_dials(), 1);

        retry.abort();
        follower.abort();
        assert!(retry.await.unwrap_err().is_cancelled());
        assert!(follower.await.unwrap_err().is_cancelled());
        dialer.set(Outcome::Connect);
        assert!(cache.connection(&context()).await.is_ok());
        assert_eq!(dialer.max_active_dials(), 1);
        assert_eq!(cache.live_count(), 1);
    }

    #[tokio::test]
    async fn service_mode_uses_the_gateway_user_as_its_single_key() {
        let dialer = Dialer::connecting();
        let cache = service_cache(&dialer);
        let first = context();
        let second = RequestContext::for_test("default", Duration::from_secs(1));

        assert_eq!(&*cache.key(&first), "gateway_svc");
        assert_eq!(cache.key(&first), cache.key(&second));
        drop(cache.connection(&first).await.unwrap());
        assert_eq!(
            dialer.dialed(),
            [Some(FlussConnectionIdentity {
                gateway_user: Arc::from("gateway_svc"),
                fluss_user: Arc::from("gateway_svc"),
            })]
        );
    }

    #[tokio::test]
    async fn plaintext_service_mode_uses_the_unauthenticated_key() {
        let dialer = Dialer::connecting();
        let cache = cache(&dialer, ClusterConfig::default());

        assert_eq!(&*cache.key(&context()), PLAINTEXT_SERVICE_USER);
        drop(cache.connection(&context()).await.unwrap());
        assert_eq!(dialer.dialed(), [None]);
    }

    #[test]
    fn access_time_never_moves_backwards() {
        let entry = CachedConnection::<FakeConnection>::new(10);

        entry.touch(20);
        entry.touch(15);

        assert_eq!(entry.last_access_ms.load(Ordering::Relaxed), 20);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_cleanup_releases_and_recreates_the_service_connection() {
        let dialer = Dialer::connecting();
        let cache = cache(
            &dialer,
            ClusterConfig {
                connection_idle_timeout: ConfigDuration::from_secs(2),
                ..service_config()
            },
        );
        drop(cache.connection(&context()).await.unwrap());

        tokio::time::advance(Duration::from_millis(1999)).await;
        cache.clean_expired().await;
        assert_eq!(dialer.closes(), 0);
        assert_eq!(cache.live_count(), 1);

        tokio::time::advance(Duration::from_millis(1)).await;
        cache.clean_expired().await;
        assert_eq!(dialer.closes(), 1);
        assert_eq!(cache.live_count(), 0);

        drop(cache.connection(&context()).await.unwrap());
        assert_eq!(dialer.dialed().len(), 2);
    }

    #[tokio::test]
    async fn close_drains_connections_and_permanently_closes_the_cache() {
        let dialer = Dialer::connecting();
        let cache = service_cache(&dialer);
        drop(cache.connection(&context()).await.unwrap());

        cache.close(Duration::from_secs(1)).await.unwrap();
        assert_eq!(dialer.closes(), 1);
        assert_eq!(cache.live_count(), 0);

        cache.close(Duration::from_secs(1)).await.unwrap();
        assert_eq!(dialer.closes(), 1);

        let error = cache.connection(&context()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert_eq!(error.message(), "the Fluss connection cache is closed");
        assert_eq!(dialer.dialed().len(), 1);
    }
}
