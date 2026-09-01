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

//! The per-request context every [`crate::backend::FlussBackend`] call carries.
//!
//! It is the whole protocol-neutral description of one request: who asked, which cluster, how long
//! there is left, and whether the caller is still waiting. Nothing in it outlives the call.

use crate::backend::types::ClusterId;
use crate::error::{GatewayError, GatewayResult};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// The caller identity produced by the Gateway authenticator.
///
/// A principal describes only the client-to-Gateway request. It is deliberately not hashable and
/// carries no Fluss connection identity: the native backend maps it immediately before choosing a
/// connection when user identity mode is implemented.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    name: Arc<str>,
    attributes: Arc<BTreeMap<String, Vec<String>>>,
}

impl Principal {
    pub fn new(name: impl Into<Arc<str>>, attributes: BTreeMap<String, Vec<String>>) -> Self {
        Self {
            name: name.into(),
            attributes: Arc::new(attributes),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn attributes(&self) -> &BTreeMap<String, Vec<String>> {
        &self.attributes
    }
}

/// One request's deadline, cancellation signal, principal, and cluster.
#[derive(Clone, Debug)]
pub struct RequestContext {
    request_id: Arc<str>,
    cluster_id: ClusterId,
    /// Absolute end of this request's budget, assigned by the REST middleware.
    deadline: Instant,
    /// Cancelled when the caller goes away or the process drains.
    cancellation: CancellationToken,
    /// `None` means the request has not been authenticated. It is not represented by a reserved
    /// principal name, so a real caller named `anonymous` remains unambiguous.
    principal: Option<Principal>,
}

impl RequestContext {
    pub fn new(
        request_id: impl Into<Arc<str>>,
        cluster_id: ClusterId,
        deadline: Instant,
        cancellation: CancellationToken,
        principal: Option<Principal>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            cluster_id,
            deadline,
            cancellation,
            principal,
        }
    }

    /// Runs one backend operation under this request's deadline and cancellation signal.
    ///
    /// Every wait a backend call can produce — queueing behind a connection attempt, the RPC itself —
    /// happens inside this future, so the caller is always answered within its budget. Abandoning the
    /// future is not the same as stopping the work: until `fluss-rs` accepts a per-RPC timeout, an RPC
    /// that outran the deadline stays in flight on its connection.
    pub async fn run<T, F>(&self, operation: F) -> GatewayResult<T>
    where
        F: Future<Output = GatewayResult<T>>,
    {
        self.ensure_active()?;
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(cancelled()),
            () = tokio::time::sleep_until(self.deadline.into()) => Err(expired()),
            result = operation => result,
        }
    }

    /// Fails fast when the caller is already gone or the budget is already spent.
    pub fn ensure_active(&self) -> GatewayResult<()> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        if Instant::now() >= self.deadline {
            return Err(expired());
        }
        Ok(())
    }

    /// What is left of the request's budget, which is what bounds connection retries.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// The absolute instant this request must be answered by.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// The authenticated caller, once authentication is implemented.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// An anonymous context with `budget` left, for the tests of the layers below the adapter.
    #[cfg(test)]
    pub(crate) fn for_test(cluster: &str, budget: Duration) -> Self {
        Self::new(
            "test-request",
            ClusterId::try_from(cluster).expect("valid test cluster ID"),
            Instant::now() + budget,
            CancellationToken::new(),
            None,
        )
    }
}

fn cancelled() -> GatewayError {
    GatewayError::cancelled("the request was cancelled")
}

fn expired() -> GatewayError {
    GatewayError::deadline_exceeded("request deadline exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn an_authenticated_principal_is_distinct_from_an_unauthenticated_request() {
        let principal = Principal::new(
            "anonymous",
            BTreeMap::from([("tenant".to_string(), vec!["sales".to_string()])]),
        );
        let ctx = RequestContext::new(
            "request",
            ClusterId::try_from("default").unwrap(),
            Instant::now() + Duration::from_secs(1),
            CancellationToken::new(),
            Some(principal),
        );

        assert_eq!(ctx.principal().unwrap().name(), "anonymous");
        assert_eq!(ctx.principal().unwrap().attributes()["tenant"], ["sales"]);
        assert!(
            RequestContext::for_test("default", Duration::from_secs(1))
                .principal()
                .is_none()
        );
    }

    /// The three outcomes `run` has to keep apart, and the fact that the operation is abandoned in two
    /// of them rather than left running.
    #[tokio::test]
    async fn run_is_bounded_by_the_deadline_and_the_cancellation_signal() {
        let ctx = RequestContext::for_test("default", Duration::from_secs(5));
        assert_eq!(ctx.run(async { Ok(7) }).await.unwrap(), 7);

        let expired = RequestContext::for_test("default", Duration::from_millis(20));
        let error: GatewayError = expired
            .run(async {
                std::future::pending::<()>().await;
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DeadlineExceeded);
        // An already-spent budget is refused without entering the operation at all.
        assert_eq!(
            expired.ensure_active().unwrap_err().kind(),
            ErrorKind::DeadlineExceeded
        );
        assert_eq!(expired.remaining(), Duration::ZERO);

        let ctx = RequestContext::for_test("default", Duration::from_secs(5));
        ctx.cancellation().cancel();
        let error: GatewayError = ctx.run(async { Ok(()) }).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cancelled);
    }
}
