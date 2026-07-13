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

use crate::client::metadata::Metadata;
use crate::error::{Error, Result};
use crate::rpc::RpcClient;
use crate::rpc::message::GetSecurityTokenRequest;
use log::{debug, info, warn};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

/// Default renewal time ratio - refresh at 80% of token lifetime
const DEFAULT_TOKEN_RENEWAL_RATIO: f64 = 0.8;
/// Default retry backoff when token fetch fails
const DEFAULT_RENEWAL_RETRY_BACKOFF: Duration = Duration::from_secs(30);
/// Minimum delay between refreshes
const MIN_RENEWAL_DELAY: Duration = Duration::from_secs(1);
/// Maximum delay between refreshes (7 days) - prevents overflow and ensures periodic refresh
const MAX_RENEWAL_DELAY: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Default refresh interval for tokens without expiration (never expires)
const DEFAULT_NON_EXPIRING_REFRESH_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 7 day

/// Type alias for credentials properties receiver
/// - `None` = not yet fetched, should wait
/// - `Some(HashMap)` = fetched (may be empty if no auth needed)
pub type CredentialsReceiver = watch::Receiver<Option<HashMap<String, String>>>;

#[derive(Debug, Deserialize)]
struct Credentials {
    access_key_id: String,
    access_key_secret: String,
    security_token: Option<String>,
}

/// Returns (opendal_key, needs_inversion)
/// needs_inversion is true for path_style_access -> enable_virtual_host_style conversion
fn convert_hadoop_key_to_opendal(hadoop_key: &str) -> Option<(String, bool)> {
    match hadoop_key {
        // S3 specific configurations
        "fs.s3a.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.s3a.endpoint.region" => Some(("region".to_string(), false)),
        "fs.s3a.path.style.access" => Some(("enable_virtual_host_style".to_string(), true)),
        "fs.s3a.connection.ssl.enabled" => None,
        // OSS specific configurations
        "fs.oss.endpoint" => Some(("endpoint".to_string(), false)),
        "fs.oss.region" => Some(("region".to_string(), false)),
        _ => None,
    }
}

/// Build remote filesystem props from credentials and additional info
fn build_remote_fs_props(
    credentials: &Credentials,
    addition_infos: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut props = HashMap::new();

    props.insert(
        "access_key_id".to_string(),
        credentials.access_key_id.clone(),
    );

    // S3 specific configurations
    props.insert(
        "secret_access_key".to_string(),
        credentials.access_key_secret.clone(),
    );

    // OSS specific configurations, todo: consider refactor it
    // to handle different conversion for different scheme in different method
    props.insert(
        "access_key_secret".to_string(),
        credentials.access_key_secret.clone(),
    );

    if let Some(token) = &credentials.security_token {
        props.insert("security_token".to_string(), token.clone());
    }

    for (key, value) in addition_infos {
        if let Some((opendal_key, transform)) = convert_hadoop_key_to_opendal(key) {
            let final_value = if transform {
                // Invert boolean value (path_style_access -> enable_virtual_host_style)
                if value == "true" {
                    "false".to_string()
                } else {
                    "true".to_string()
                }
            } else {
                value.clone()
            };
            props.insert(opendal_key, final_value);
        }
    }

    props
}

/// Manager for security tokens that refreshes tokens in a background task.
///
/// This follows the pattern from Java's `DefaultSecurityTokenManager`, where
/// a background thread periodically refreshes tokens based on their expiration time.
///
/// Uses `tokio::sync::watch` channel to broadcast token updates to consumers.
/// Consumers can subscribe by calling `subscribe()` to get a receiver.
///
/// The channel value is `Option<HashMap>`:
/// - `None` = not yet fetched, consumers should wait
/// - `Some(HashMap)` = fetched (may be empty if no auth needed)
///
/// # Example
/// ```ignore
/// let manager = SecurityTokenManager::new(rpc_client, metadata);
/// let credentials_rx = manager.subscribe();
/// manager.start();
///
/// // Consumer can get latest credentials via:
/// let props = credentials_rx.borrow().clone();
/// ```
pub struct SecurityTokenManager {
    rpc_client: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    token_renewal_ratio: f64,
    renewal_retry_backoff: Duration,
    /// Watch channel sender for broadcasting token updates
    credentials_tx: watch::Sender<Option<HashMap<String, String>>>,
    /// Watch channel receiver (kept to allow cloning for new subscribers)
    credentials_rx: watch::Receiver<Option<HashMap<String, String>>>,
    /// Handle to the background refresh task
    task_handle: RwLock<Option<JoinHandle<()>>>,
    /// Sender to signal shutdown
    shutdown_tx: RwLock<Option<oneshot::Sender<()>>>,
}

impl SecurityTokenManager {
    pub fn new(rpc_client: Arc<RpcClient>, metadata: Arc<Metadata>) -> Self {
        let (credentials_tx, credentials_rx) = watch::channel(None);
        Self {
            rpc_client,
            metadata,
            token_renewal_ratio: DEFAULT_TOKEN_RENEWAL_RATIO,
            renewal_retry_backoff: DEFAULT_RENEWAL_RETRY_BACKOFF,
            credentials_tx,
            credentials_rx,
            task_handle: RwLock::new(None),
            shutdown_tx: RwLock::new(None),
        }
    }

    /// Subscribe to credential updates.
    /// Returns a receiver that always contains the latest credentials.
    /// Consumers can call `receiver.borrow()` to get the current value.
    pub fn subscribe(&self) -> CredentialsReceiver {
        self.credentials_rx.clone()
    }

    /// Start the background token refresh task.
    /// This should be called once after creating the manager.
    pub fn start(&self) {
        if self.task_handle.read().is_some() {
            warn!("SecurityTokenManager is already started");
            return;
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.write() = Some(shutdown_tx);

        let rpc_client = Arc::clone(&self.rpc_client);
        let metadata = Arc::clone(&self.metadata);
        let token_renewal_ratio = self.token_renewal_ratio;
        let renewal_retry_backoff = self.renewal_retry_backoff;
        let credentials_tx = self.credentials_tx.clone();

        let handle = tokio::spawn(async move {
            Self::token_refresh_loop(
                rpc_client,
                metadata,
                token_renewal_ratio,
                renewal_retry_backoff,
                credentials_tx,
                shutdown_rx,
            )
            .await;
        });

        *self.task_handle.write() = Some(handle);
        info!("SecurityTokenManager started");
    }

    /// Stop the background token refresh task.
    pub fn stop(&self) {
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        // Take and drop the task handle so the task can finish gracefully
        let _ = self.task_handle.write().take();
        info!("SecurityTokenManager stopped");
    }

    /// Background task that periodically refreshes tokens.
    async fn token_refresh_loop(
        rpc_client: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        token_renewal_ratio: f64,
        renewal_retry_backoff: Duration,
        credentials_tx: watch::Sender<Option<HashMap<String, String>>>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        info!("Starting token refresh loop");

        loop {
            // Fetch token and send to channel
            let result = Self::fetch_token(&rpc_client, &metadata).await;

            let next_delay = match result {
                Ok((props, expiration_time)) => {
                    // Send credentials via watch channel (Some indicates fetched)
                    if let Err(e) = credentials_tx.send(Some(props)) {
                        debug!("No active subscribers for credentials update: {e:?}");
                    }

                    // Calculate next renewal delay based on expiration time
                    if let Some(exp_time) = expiration_time {
                        Self::calculate_renewal_delay(exp_time, token_renewal_ratio)
                    } else {
                        // No expiration time - token never expires, use long refresh interval
                        info!(
                            "Token has no expiration time (never expires), next refresh in {DEFAULT_NON_EXPIRING_REFRESH_INTERVAL:?}"
                        );
                        DEFAULT_NON_EXPIRING_REFRESH_INTERVAL
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to obtain security token: {e:?}, will retry in {renewal_retry_backoff:?}"
                    );
                    renewal_retry_backoff
                }
            };

            debug!("Next token refresh in {next_delay:?}");

            // Wait for either the delay to elapse or shutdown signal
            tokio::select! {
                _ = tokio::time::sleep(next_delay) => {
                    // Continue to next iteration to refresh
                }
                _ = &mut shutdown_rx => {
                     info!("Token refresh loop received shutdown signal");
                    break;
                }
            }
        }
    }

    /// Fetch token from server.
    /// Returns the props and expiration time if available.
    async fn fetch_token(
        rpc_client: &Arc<RpcClient>,
        metadata: &Arc<Metadata>,
    ) -> Result<(HashMap<String, String>, Option<i64>)> {
        let cluster = metadata.get_cluster();
        let server_node =
            cluster
                .get_one_available_server()
                .ok_or_else(|| Error::UnexpectedError {
                    message: "No tablet server available for token refresh".to_string(),
                    source: None,
                })?;

        let conn = rpc_client.get_connection(server_node).await?;
        let request = GetSecurityTokenRequest::new();
        let response = conn.request(request).await?;

        // The token may be empty if remote filesystem doesn't require authentication
        if response.token.is_empty() {
            info!("Empty token received, remote filesystem may not require authentication");
            return Ok((HashMap::new(), response.expiration_time));
        }

        let credentials: Credentials =
            serde_json::from_slice(&response.token).map_err(|e| Error::JsonSerdeError {
                message: format!("Error when parsing token from server: {e}"),
            })?;

        let mut addition_infos = HashMap::new();
        for kv in &response.addition_info {
            addition_infos.insert(kv.key.clone(), kv.value.clone());
        }

        let props = build_remote_fs_props(&credentials, &addition_infos);
        debug!("Security token fetched successfully");

        Ok((props, response.expiration_time))
    }

    /// Calculate the delay before next token renewal.
    /// Uses the renewal ratio to refresh before actual expiration.
    /// Caps the delay to MAX_RENEWAL_DELAY to prevent overflow and ensure periodic refresh.
    fn calculate_renewal_delay(expiration_time: i64, renewal_ratio: f64) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let time_until_expiry = expiration_time - now;
        if time_until_expiry <= 0 {
            // Token already expired, refresh immediately
            return MIN_RENEWAL_DELAY;
        }

        // Cap time_until_expiry to prevent overflow when casting to f64 and back
        let max_delay_ms = MAX_RENEWAL_DELAY.as_millis() as i64;
        let capped_time = time_until_expiry.min(max_delay_ms);

        let delay_ms = (capped_time as f64 * renewal_ratio) as u64;
        let delay = Duration::from_millis(delay_ms);

        debug!(
            "Calculated renewal delay: {delay:?} (expiration: {expiration_time}, now: {now}, ratio: {renewal_ratio})"
        );

        delay.clamp(MIN_RENEWAL_DELAY, MAX_RENEWAL_DELAY)
    }
}

impl Drop for SecurityTokenManager {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_hadoop_key_to_opendal_maps_known_keys() {
        // S3 keys
        let (key, invert) = convert_hadoop_key_to_opendal("fs.s3a.endpoint").expect("key");
        assert_eq!(key, "endpoint");
        assert!(!invert);

        let (key, invert) = convert_hadoop_key_to_opendal("fs.s3a.path.style.access").expect("key");
        assert_eq!(key, "enable_virtual_host_style");
        assert!(invert);

        assert!(convert_hadoop_key_to_opendal("fs.s3a.connection.ssl.enabled").is_none());

        // OSS keys
        let (key, invert) = convert_hadoop_key_to_opendal("fs.oss.endpoint").expect("key");
        assert_eq!(key, "endpoint");
        assert!(!invert);

        let (key, invert) = convert_hadoop_key_to_opendal("fs.oss.region").expect("key");
        assert_eq!(key, "region");
        assert!(!invert);

        // Unknown key
        assert!(convert_hadoop_key_to_opendal("unknown.key").is_none());
    }

    #[test]
    fn calculate_renewal_delay_returns_correct_delay() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Token expires in 1 hour
        let expiration = now + 3600 * 1000;
        let delay = SecurityTokenManager::calculate_renewal_delay(expiration, 0.8);

        // Should be approximately 48 minutes (80% of 1 hour)
        let expected_min = Duration::from_secs(2800); // ~46.7 minutes
        let expected_max = Duration::from_secs(2900); // ~48.3 minutes
        assert!(
            delay >= expected_min && delay <= expected_max,
            "Expected delay between {expected_min:?} and {expected_max:?}, got {delay:?}"
        );
    }

    #[test]
    fn calculate_renewal_delay_handles_expired_token() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Token already expired
        let expiration = now - 1000;
        let delay = SecurityTokenManager::calculate_renewal_delay(expiration, 0.8);

        // Should return minimum delay
        assert_eq!(delay, MIN_RENEWAL_DELAY);
    }

    #[test]
    fn build_remote_fs_props_includes_all_fields() {
        let credentials = Credentials {
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            security_token: Some("token".to_string()),
        };
        let addition_infos =
            HashMap::from([("fs.s3a.path.style.access".to_string(), "true".to_string())]);

        let props = build_remote_fs_props(&credentials, &addition_infos);
        assert_eq!(props.get("access_key_id"), Some(&"ak".to_string()));
        assert_eq!(props.get("access_key_secret"), Some(&"sk".to_string()));
        assert_eq!(props.get("access_key_secret"), Some(&"sk".to_string()));
        assert_eq!(props.get("security_token"), Some(&"token".to_string()));
        assert_eq!(
            props.get("enable_virtual_host_style"),
            Some(&"false".to_string())
        );
    }
}
