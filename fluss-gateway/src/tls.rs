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

//! TLS identities shared by protocol listeners. This module owns certificates and handshakes,
//! while each listener chooses its ALPN policy and when to begin TLS on a socket.

use crate::config::{ConfigError, TlsProfileConfig};
use std::collections::BTreeMap;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::rustls;
use tokio_rustls::{TlsAcceptor, server::TlsStream};

/// The TLS policy selected by one protocol listener.
#[derive(Clone)]
pub struct TlsListener {
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
}

impl TlsListener {
    /// Complete a server handshake within this profile's deadline. The protocol listener owns
    /// cancellation, so it can stop an accepted connection as part of its own shutdown.
    pub async fn accept(&self, socket: TcpStream) -> std::io::Result<TlsStream<TcpStream>> {
        tokio::time::timeout(self.handshake_timeout, self.acceptor.accept(socket))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake timed out")
            })?
    }
}

/// Metadata derived from the accepted transport, never from client-supplied HTTP headers.
#[derive(Clone, Debug)]
pub struct ConnectionMetadata {
    pub peer_addr: SocketAddr,
    pub tls: bool,
    pub negotiated_alpn: Option<Vec<u8>>,
    /// DER certificates accepted by rustls' client-auth verifier, if one is configured.
    pub peer_certificates_der: Vec<Vec<u8>>,
}

impl ConnectionMetadata {
    pub fn plaintext(peer_addr: SocketAddr) -> Self {
        Self {
            peer_addr,
            tls: false,
            negotiated_alpn: None,
            peer_certificates_der: Vec::new(),
        }
    }

    pub fn secure(peer_addr: SocketAddr, stream: &TlsStream<TcpStream>) -> Self {
        Self {
            peer_addr,
            tls: true,
            negotiated_alpn: stream.get_ref().1.alpn_protocol().map(ToOwned::to_owned),
            peer_certificates_der: stream
                .get_ref()
                .1
                .peer_certificates()
                .into_iter()
                .flatten()
                .map(|cert| cert.as_ref().to_vec())
                .collect(),
        }
    }
}

/// Validated certificates and keys, loaded once before any listener binds.
pub struct TlsProfiles {
    profiles: BTreeMap<String, (Arc<rustls::ServerConfig>, Duration)>,
}

impl TlsProfiles {
    pub fn load(profiles: &BTreeMap<String, TlsProfileConfig>) -> Result<Self, ConfigError> {
        let mut loaded = BTreeMap::new();
        for (name, profile) in profiles {
            let certificate = std::fs::read(&profile.certificate_file).map_err(|error| {
                invalid(
                    name,
                    "certificate-file",
                    format!("cannot read file: {error}"),
                )
            })?;
            let private_key = std::fs::read(&profile.private_key_file).map_err(|error| {
                invalid(
                    name,
                    "private-key-file",
                    format!("cannot read file: {error}"),
                )
            })?;
            let certificates = rustls_pemfile::certs(&mut BufReader::new(certificate.as_slice()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| invalid(name, "certificate-file", error.to_string()))?;
            if certificates.is_empty() {
                return Err(invalid(
                    name,
                    "certificate-file",
                    "no PEM certificates".into(),
                ));
            }
            let key = rustls_pemfile::private_key(&mut BufReader::new(private_key.as_slice()))
                .map_err(|error| invalid(name, "private-key-file", error.to_string()))?
                .ok_or_else(|| invalid(name, "private-key-file", "no PEM private key".into()))?;
            let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|error| invalid(name, "certificate-file", error.to_string()))?
            .with_no_client_auth()
            .with_single_cert(certificates, key)
            .map_err(|error| invalid(name, "private-key-file", error.to_string()))?;
            loaded.insert(
                name.clone(),
                (Arc::new(server_config), profile.handshake_timeout.get()),
            );
        }
        Ok(Self { profiles: loaded })
    }

    /// A protocol selects its ALPN list independently of the shared certificate identity.
    pub fn listener(&self, name: &str, alpn: &[&[u8]]) -> Option<TlsListener> {
        self.profiles
            .get(name)
            .map(|(server_config, handshake_timeout)| {
                let mut server_config = (**server_config).clone();
                server_config.alpn_protocols =
                    alpn.iter().map(|protocol| protocol.to_vec()).collect();
                TlsListener {
                    acceptor: TlsAcceptor::from(Arc::new(server_config)),
                    handshake_timeout: *handshake_timeout,
                }
            })
    }
}

fn invalid(name: &str, field: &str, detail: String) -> ConfigError {
    ConfigError::Invalid(vec![format!(
        "gateway.tls.profile.{name}.{field}: {detail}"
    )])
}
