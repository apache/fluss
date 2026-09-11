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

use super::{RpcClient, server_connection_from_duplex};
use crate::cluster::ServerNode;
use crate::proto::ErrorResponse;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

pub(crate) struct FramedRequest {
    pub(crate) api_key: i16,
    pub(crate) api_version: i16,
    pub(crate) request_id: i32,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn install_duplex_connection(
    rpc_client: &RpcClient,
    server_node: &ServerNode,
) -> DuplexStream {
    let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
    rpc_client
        .insert_connection_for_test(server_node, server_connection_from_duplex(client_stream));
    server_stream
}

pub(crate) async fn read_framed_request(stream: &mut DuplexStream) -> FramedRequest {
    let length = stream.read_i32().await.expect("request frame length");
    assert!(length >= 8, "request frame must contain its header");
    let mut payload = vec![0; length as usize];
    stream
        .read_exact(&mut payload)
        .await
        .expect("request frame payload");
    FramedRequest {
        api_key: i16::from_be_bytes([payload[0], payload[1]]),
        api_version: i16::from_be_bytes([payload[2], payload[3]]),
        request_id: i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]),
        body: payload[8..].to_vec(),
    }
}

pub(crate) async fn write_success_response(
    stream: &mut DuplexStream,
    request_id: i32,
    response: &impl Message,
) {
    let mut payload = Vec::new();
    payload.push(0);
    payload.extend_from_slice(&request_id.to_be_bytes());
    response
        .encode(&mut payload)
        .expect("encode successful response");
    write_response_frame(stream, payload).await;
}

pub(crate) async fn write_error_response(
    stream: &mut DuplexStream,
    request_id: i32,
    code: i32,
    message: &str,
) {
    let mut payload = Vec::new();
    payload.push(1);
    payload.extend_from_slice(&request_id.to_be_bytes());
    ErrorResponse {
        error_code: code,
        error_message: Some(message.to_string()),
    }
    .encode(&mut payload)
    .expect("encode error response");
    write_response_frame(stream, payload).await;
}

async fn write_response_frame(stream: &mut DuplexStream, payload: Vec<u8>) {
    stream
        .write_i32(payload.len() as i32)
        .await
        .expect("response frame length");
    stream
        .write_all(&payload)
        .await
        .expect("response frame payload");
    stream.flush().await.expect("flush response");
}
