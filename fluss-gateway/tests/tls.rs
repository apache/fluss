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

//! Real-socket HTTPS checks for the production listener and shared TLS profiles.

use fluss_gateway::config::{ConfigDuration, GatewayConfig, TlsProfileConfig};
use fluss_gateway::lifecycle;
use fluss_gateway::tls::{ConnectionMetadata, TlsProfiles};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{self, RootCertStore, pki_types::ServerName};

fn identity(dir: &tempfile::TempDir, name: &str) -> (String, String, Vec<u8>) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate");
    let cert_path = dir.path().join(format!("{name}.pem"));
    let key_path = dir.path().join(format!("{name}.key"));
    std::fs::write(&cert_path, cert.pem()).expect("write certificate");
    std::fs::write(&key_path, key_pair.serialize_pem()).expect("write key");
    (
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
        cert.der().to_vec(),
    )
}

fn config_with_profile(
    name: &str,
    certificate_file: String,
    private_key_file: String,
) -> GatewayConfig {
    let mut config = GatewayConfig::default();
    config.server.rest.bind_address = "127.0.0.1:0".parse().unwrap();
    config.server.metrics.enabled = false;
    config.server.rest.tls_profile = Some(name.to_string());
    config.tls_profiles.insert(
        name.to_string(),
        TlsProfileConfig {
            certificate_file,
            private_key_file,
            handshake_timeout: ConfigDuration::from_secs(2),
        },
    );
    config
}

fn client_connector(certificate: &[u8]) -> Result<TlsConnector, String> {
    let mut roots = RootCertStore::empty();
    roots
        .add(certificate.to_vec().into())
        .map_err(|error| error.to_string())?;
    let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| error.to_string())?
    .with_root_certificates(roots)
    .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(client)))
}

async fn https_get(address: SocketAddr, certificate: &[u8]) -> Result<String, String> {
    let connector = client_connector(certificate)?;
    let socket = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|error| error.to_string())?;
    let mut socket = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .map_err(|error| error.to_string())?;
    socket
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut response))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

#[tokio::test]
async fn connection_metadata_comes_from_the_tls_session() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, der) = identity(&dir, "server");
    let config = config_with_profile("public", cert, key);
    let profiles = TlsProfiles::load(&config.tls_profiles).unwrap();
    let tls = profiles.listener("public", &[b"http/1.1"]).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, peer_addr) = listener.accept().await.unwrap();
        let stream = tls.accept(socket).await.unwrap();
        ConnectionMetadata::secure(peer_addr, &stream)
    });
    let connector = client_connector(&der).unwrap();
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let _client = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    let metadata = server.await.unwrap();
    assert!(metadata.tls);
    assert!(metadata.peer_addr.ip().is_loopback());
    assert_eq!(
        metadata.negotiated_alpn.as_deref(),
        Some(b"http/1.1".as_slice())
    );
    assert!(metadata.peer_certificates_der.is_empty());
}

#[tokio::test]
async fn https_serves_health_and_plaintext_cannot_get_an_http_response() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, cert_der) = identity(&dir, "server");
    let gateway = lifecycle::start(config_with_profile("public", cert, key))
        .await
        .expect("HTTPS gateway starts");
    let address = gateway.local_addr();
    let response = https_get(address, &cert_der).await.expect("trusted HTTPS");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"status\":\"ok\""), "{response}");

    let mut plaintext = tokio::net::TcpStream::connect(address).await.unwrap();
    plaintext
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = [0; 256];
    let result = tokio::time::timeout(Duration::from_secs(2), plaintext.read(&mut bytes))
        .await
        .expect("plaintext connection closes");
    assert!(
        !matches!(result, Ok(count) if bytes[..count].starts_with(b"HTTP/1.1 200")),
        "plaintext must not bypass TLS"
    );
    gateway.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn invalid_certificate_or_mismatched_key_fails_before_binding() {
    let dir = tempfile::tempdir().unwrap();
    let (cert_a, _, _) = identity(&dir, "a");
    let (_, key_b, _) = identity(&dir, "b");
    let config = config_with_profile("public", cert_a, key_b);
    let error = lifecycle::start(config)
        .await
        .err()
        .expect("mismatch rejected");
    assert!(
        error
            .to_string()
            .contains("gateway.tls.profile.public.private-key-file")
    );

    let invalid_path = dir.path().join("invalid.pem");
    std::fs::write(&invalid_path, b"not a PEM certificate").unwrap();
    let (_, key, _) = identity(&dir, "c");
    let config = config_with_profile("public", invalid_path.to_string_lossy().into_owned(), key);
    let error = lifecycle::start(config)
        .await
        .err()
        .expect("invalid certificate rejected");
    assert!(
        error
            .to_string()
            .contains("gateway.tls.profile.public.certificate-file")
    );
}

#[tokio::test]
async fn shared_profiles_keep_distinct_certificates_and_client_trust() {
    let dir = tempfile::tempdir().unwrap();
    let (cert_a, key_a, der_a) = identity(&dir, "a");
    let (cert_b, key_b, der_b) = identity(&dir, "b");
    let mut config = config_with_profile("a", cert_a, key_a);
    config.tls_profiles.insert(
        "b".to_string(),
        TlsProfileConfig {
            certificate_file: cert_b,
            private_key_file: key_b,
            handshake_timeout: ConfigDuration::from_secs(2),
        },
    );
    let gateway_a = lifecycle::start(config.clone()).await.expect("profile a");
    assert!(https_get(gateway_a.local_addr(), &der_a).await.is_ok());
    assert!(https_get(gateway_a.local_addr(), &der_b).await.is_err());
    gateway_a.shutdown().await.unwrap();

    config.server.rest.tls_profile = Some("b".to_string());
    let gateway_b = lifecycle::start(config).await.expect("profile b");
    assert!(https_get(gateway_b.local_addr(), &der_b).await.is_ok());
    assert!(https_get(gateway_b.local_addr(), &der_a).await.is_err());
    gateway_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn incomplete_handshake_is_closed_within_configured_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, _) = identity(&dir, "server");
    let mut config = config_with_profile("public", cert, key);
    config
        .tls_profiles
        .get_mut("public")
        .unwrap()
        .handshake_timeout = ConfigDuration::from_millis(150);
    let gateway = lifecycle::start(config).await.expect("start");
    let mut socket = tokio::net::TcpStream::connect(gateway.local_addr())
        .await
        .expect("connect");
    let mut byte = [0];
    let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
        .await
        .expect("TLS handshake is bounded");
    gateway.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn shutdown_cancels_a_stalled_tls_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, _) = identity(&dir, "server");
    let mut config = config_with_profile("public", cert, key);
    config
        .tls_profiles
        .get_mut("public")
        .unwrap()
        .handshake_timeout = ConfigDuration::from_secs(30);
    let gateway = lifecycle::start(config).await.expect("start");
    let _stalled = tokio::net::TcpStream::connect(gateway.local_addr())
        .await
        .expect("connect");
    tokio::time::timeout(Duration::from_secs(3), gateway.shutdown())
        .await
        .expect("shutdown must not wait for the handshake budget")
        .expect("clean shutdown");
}

#[tokio::test]
async fn https_keeps_the_http_header_read_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, der) = identity(&dir, "server");
    let mut config = config_with_profile("public", cert, key);
    config.server.rest.header_read_timeout = ConfigDuration::from_millis(150);
    let gateway = lifecycle::start(config).await.expect("start");
    let connector = client_connector(&der).unwrap();
    let socket = tokio::net::TcpStream::connect(gateway.local_addr())
        .await
        .unwrap();
    let mut socket = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    socket.write_all(b"GET /heal").await.unwrap();
    let mut byte = [0];
    let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
        .await
        .expect("incomplete HTTP head is closed after its deadline");
    gateway.shutdown().await.expect("clean shutdown");
}
