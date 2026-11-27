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

use super::Metadata;
use crate::cluster::ServerNode;
use crate::error::{Error, FlussError, Result};
use crate::proto::MetadataResponse;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::rpc::{RpcClient, RpcError};
use arc_swap::ArcSwap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::OnceCell;

#[derive(Clone, Copy, Debug)]
pub(super) enum GatewayTarget {
    TabletServer,
    Coordinator,
}

impl GatewayTarget {
    pub(super) fn is_available(self, response: &MetadataResponse) -> bool {
        match self {
            Self::TabletServer => !response.tablet_servers.is_empty(),
            Self::Coordinator => response.coordinator_server.is_some(),
        }
    }

    pub(super) fn needs_refresh(error: &Error) -> bool {
        error.is_retriable() || matches!(error, Error::InvalidServerType { .. })
    }

    fn can_replay(self, error: &Error) -> bool {
        // A standby rejects the RPC before applying it; a role mismatch fails the handshake.
        // A lost response, on the other hand, can hide a successful coordinator mutation.
        matches!(self, Self::TabletServer)
            || matches!(error, Error::InvalidServerType { .. })
            || error.api_error() == Some(FlussError::NotCoordinatorLeaderException)
    }
}

type RefreshRound = OnceCell<std::result::Result<Option<ServerNode>, ()>>;

pub(super) struct RetryableGateway {
    target: GatewayTarget,
    metadata: Arc<Metadata>,
    client: Arc<RpcClient>,
    refresh: ArcSwap<RefreshRound>,
}

impl RetryableGateway {
    pub(super) fn new(
        target: GatewayTarget,
        metadata: Arc<Metadata>,
        client: Arc<RpcClient>,
    ) -> Self {
        Self {
            target,
            metadata,
            client,
            refresh: ArcSwap::from_pointee(OnceCell::new()),
        }
    }

    fn select(&self, responding_tablet: Option<&ServerNode>) -> Result<ServerNode> {
        let cluster = self.metadata.get_cluster();
        let (server, missing) = match self.target {
            // A recovered peer may still advertise stale addresses. Prefer the tablet that
            // answered discovery only while its endpoint still matches this snapshot.
            GatewayTarget::TabletServer => (
                responding_tablet
                    .filter(|t| cluster.get_tablet_server(t.id()) == Some(*t))
                    .or_else(|| cluster.get_one_available_server()),
                "No available tablet server for a read-only Admin request",
            ),
            GatewayTarget::Coordinator => (
                cluster.get_coordinator_server(),
                "No coordinator in cluster metadata",
            ),
        };
        server
            .cloned()
            .ok_or_else(|| RpcError::ConnectionError(missing.into()).into())
    }

    pub(super) async fn request<R>(&self, request: R) -> Result<R::ResponseBody>
    where
        R: RequestBody + Sync + WriteType<Vec<u8>>,
        R::ResponseBody: ReadType<Cursor<Vec<u8>>>,
    {
        let request = &request;
        self.call(|server| async move {
            self.client
                .get_connection(&server)
                .await?
                .request(request)
                .await
        })
        .await
    }

    pub(super) async fn call<T, F: Future<Output = Result<T>>>(
        &self,
        send: impl Fn(ServerNode) -> F,
    ) -> Result<T> {
        // Capture the round before sending: even a late failure from the same batch can
        // reuse discovery already completed by a sibling.
        let round = self.refresh.load_full();
        let (error, replay) = match self.select(None) {
            Ok(server) => match send(server).await {
                Ok(response) => return Ok(response),
                Err(e) if GatewayTarget::needs_refresh(&e) => {
                    let replay = self.target.can_replay(&e);
                    (e, replay)
                }
                Err(e) => return Err(e),
            },
            // No destination was selected, so nothing has been sent, even for a mutation.
            Err(e) => (e, true),
        };
        let refreshed = round
            .get_or_init(|| async {
                let result = self.metadata.refresh_server_nodes(Some(self.target)).await;
                self.refresh.store(Arc::new(OnceCell::new()));
                result.map_err(|e| log::warn!("Failed to refresh {:?}: {e}", self.target))
            })
            .await;
        match refreshed {
            Ok(tablet) if replay => send(self.select(tablet.as_ref())?).await,
            _ => Err(error),
        }
    }
}
