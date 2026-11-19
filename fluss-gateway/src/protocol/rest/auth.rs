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

use super::{error_response, request_id};
use crate::auth::{
    AuthenticationError, Authenticator, Credential, MAX_CREDENTIAL_BYTES, valid_principal,
};
use crate::config::{AuthenticationMode, Secret, SecurityConfig};
use crate::error::GatewayError;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue, header};
use axum::middleware::Next;
use axum::response::Response;
use axum::{Router, middleware};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

pub struct HttpAuthentication {
    mode: AuthenticationMode,
    authenticator: Arc<dyn Authenticator>,
    identity_header: HeaderName,
    proxies: BTreeSet<IpAddr>,
}

pub(crate) fn validate_config(config: &SecurityConfig, loopback: bool) -> Result<(), String> {
    crate::auth::validate_config(config)?;
    if matches!(
        config.authentication,
        AuthenticationMode::Password | AuthenticationMode::Token
    ) && !loopback
        && !config.allow_insecure_transport
    {
        return Err("gateway.security.allow-insecure-transport must be true for password/token authentication on a non-loopback plaintext listener".into());
    }
    if config.authentication == AuthenticationMode::TrustedHeader {
        identity_header(config)?;
    }
    Ok(())
}

fn identity_header(config: &SecurityConfig) -> Result<HeaderName, String> {
    if config.trusted_proxy_addresses.is_empty() {
        return Err("gateway.security.trusted-header.proxy-addresses must contain at least one trusted peer IP".into());
    }
    let name = HeaderName::from_bytes(config.trusted_header_name().as_bytes())
        .map_err(|_| "gateway.security.trusted-header.name must be a legal HTTP header name")?;
    if [
        "authorization",
        "proxy-authorization",
        "www-authenticate",
        "proxy-authenticate",
        "cookie",
        "set-cookie",
        "connection",
        "keep-alive",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "content-type",
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-proto",
        "x-request-id",
    ]
    .contains(&name.as_str())
    {
        return Err("gateway.security.trusted-header.name must not be a credential, routing, or hop-by-hop header".into());
    }
    Ok(name)
}

impl HttpAuthentication {
    pub fn new(
        config: &SecurityConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> Result<Arc<Self>, String> {
        let identity_header = if config.authentication == AuthenticationMode::TrustedHeader {
            identity_header(config)?
        } else {
            HeaderName::from_static("x-forwarded-user")
        };
        Ok(Arc::new(Self {
            mode: config.authentication,
            authenticator,
            identity_header,
            proxies: config
                .trusted_proxy_addresses
                .iter()
                .map(IpAddr::to_canonical)
                .collect(),
        }))
    }

    fn extract(&self, request: &Request) -> Result<Credential, AuthenticationError> {
        if self.mode == AuthenticationMode::TrustedHeader {
            let peer = request
                .extensions()
                .get::<SocketAddr>()
                .ok_or(AuthenticationError::Forbidden)?
                .ip();
            if !self.proxies.contains(&peer.to_canonical()) {
                return Err(AuthenticationError::Forbidden);
            }
            for value in request.headers().get_all(header::CONNECTION) {
                let value = value.to_str().map_err(|_| AuthenticationError::Forbidden)?;
                if value.split(',').any(|token| {
                    token
                        .trim()
                        .eq_ignore_ascii_case(self.identity_header.as_str())
                }) {
                    return Err(AuthenticationError::Forbidden);
                }
            }
            let name = single_header(request.headers(), &self.identity_header)
                .map_err(|_| AuthenticationError::Forbidden)?
                .ok_or(AuthenticationError::Forbidden)?;
            if !valid_principal(name) || name.contains(',') {
                return Err(AuthenticationError::Forbidden);
            }
            return Ok(Credential::Trust {
                username: Some(name.to_owned()),
            });
        }

        let authorization = single_header(request.headers(), &header::AUTHORIZATION)?;
        let Some(value) = authorization else {
            return if self.mode == AuthenticationMode::Trust {
                Ok(Credential::Trust { username: None })
            } else {
                Err(AuthenticationError::Unauthenticated)
            };
        };
        let (scheme, value) = value
            .split_once(' ')
            .ok_or(AuthenticationError::Unauthenticated)?;
        let value = value.trim_start_matches(' ');
        if matches!(
            self.mode,
            AuthenticationMode::Trust | AuthenticationMode::Password
        ) && scheme.eq_ignore_ascii_case("basic")
        {
            let decoded = STANDARD
                .decode(value)
                .map_err(|_| AuthenticationError::Unauthenticated)?;
            let decoded =
                std::str::from_utf8(&decoded).map_err(|_| AuthenticationError::Unauthenticated)?;
            let (username, password) = decoded
                .split_once(':')
                .ok_or(AuthenticationError::Unauthenticated)?;
            if !(valid_principal(username)
                || (username.is_empty() && self.mode == AuthenticationMode::Trust))
                || password.chars().any(char::is_control)
            {
                return Err(AuthenticationError::Unauthenticated);
            }
            return if self.mode == AuthenticationMode::Trust {
                Ok(Credential::Trust {
                    username: Some(username.to_owned()),
                })
            } else {
                Ok(Credential::Password {
                    username: username.to_owned(),
                    password: Secret::new(password),
                })
            };
        }
        if self.mode == AuthenticationMode::Token
            && scheme.eq_ignore_ascii_case("bearer")
            && crate::auth::valid_token(value)
        {
            return Ok(Credential::Bearer(Secret::new(value)));
        }
        Err(AuthenticationError::Unauthenticated)
    }

    fn failure(&self, error: AuthenticationError, request: &Request) -> Response {
        let mapped = match error {
            AuthenticationError::Unauthenticated
                if self.mode != AuthenticationMode::TrustedHeader =>
            {
                GatewayError::unauthenticated("authentication failed")
            }
            AuthenticationError::Unauthenticated | AuthenticationError::Forbidden => {
                GatewayError::unauthorized("authentication policy rejected the request")
            }
            AuthenticationError::ResourceExhausted => {
                GatewayError::resource_exhausted("too many authentication verifications")
            }
            AuthenticationError::Internal => {
                GatewayError::internal("authentication failed internally")
            }
        };
        let mut response = error_response(&mapped, &request_id(request));
        if error == AuthenticationError::Unauthenticated
            && self.mode != AuthenticationMode::TrustedHeader
        {
            let challenge = if self.mode == AuthenticationMode::Token {
                "Bearer realm=\"fluss-gateway\""
            } else {
                "Basic realm=\"fluss-gateway\", charset=\"UTF-8\""
            };
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static(challenge),
            );
        }
        response
    }

    async fn authenticate(&self, mut request: Request, next: Next) -> Response {
        let credential = match self.extract(&request) {
            Ok(credential) => credential,
            Err(error) => return self.failure(error, &request),
        };
        match self.authenticator.authenticate(credential).await {
            Ok(principal)
                if valid_principal(principal.name())
                    || (self.mode == AuthenticationMode::Trust
                        && principal.name() == "anonymous") =>
            {
                request.headers_mut().remove(header::AUTHORIZATION);
                request.headers_mut().remove(&self.identity_header);
                request.extensions_mut().insert(principal);
                next.run(request).await
            }
            Ok(_) => self.failure(AuthenticationError::Internal, &request),
            Err(error) => self.failure(error, &request),
        }
    }

    pub(crate) fn describe(&self, document: &mut Value) {
        let (name, scheme) = match self.mode {
            AuthenticationMode::Trust => (
                "basicAuth",
                json!({"type":"http", "scheme":"basic", "description":"Optional username in trust mode. Missing or empty username uses an anonymous identity; passwords are not verified."}),
            ),
            AuthenticationMode::Password => ("basicAuth", json!({"type":"http", "scheme":"basic"})),
            AuthenticationMode::Token => ("bearerAuth", json!({"type":"http", "scheme":"bearer"})),
            AuthenticationMode::TrustedHeader => (
                "trustedProxy",
                json!({"type":"apiKey", "in":"header", "name":self.identity_header.as_str(), "description":"Accepted only from configured trusted proxy peer IPs."}),
            ),
        };
        document["components"]["securitySchemes"][name] = scheme;
        let requirement = json!({name: []});
        document["security"] = if self.mode == AuthenticationMode::Trust {
            json!([{}, requirement])
        } else {
            json!([requirement])
        };
        let status = if self.mode == AuthenticationMode::TrustedHeader {
            "403"
        } else {
            "401"
        };
        let mut response = json!({
            "description":"Authentication failed",
            "content":{"application/json":{"schema":{"$ref":"#/components/schemas/ErrorEnvelope"}}}
        });
        if status == "401" {
            response["headers"] = json!({"WWW-Authenticate":{
                "description":"Challenge for the configured authentication scheme",
                "schema":{"type":"string"}
            }});
        }
        document["components"]["responses"]["AuthenticationFailed"] = response;
        for (path, item) in document["paths"].as_object_mut().expect("OpenAPI paths") {
            for (method, operation) in item.as_object_mut().expect("path item") {
                if !["get", "post", "put", "patch", "delete", "head", "options"]
                    .contains(&method.as_str())
                {
                    continue;
                }
                if path == "/health" || path == "/ready" {
                    operation["security"] = json!([]);
                } else if operation["responses"].get(status).is_none() {
                    operation["responses"][status] =
                        json!({"$ref":"#/components/responses/AuthenticationFailed"});
                }
            }
        }
    }
}

fn single_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
) -> Result<Option<&'a str>, AuthenticationError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.as_bytes().len() > MAX_CREDENTIAL_BYTES {
        return Err(AuthenticationError::Unauthenticated);
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| AuthenticationError::Unauthenticated)
}

pub(crate) fn apply(router: Router, binding: Arc<HttpAuthentication>) -> Router {
    router
        .route_layer(middleware::from_fn(move |request: Request, next: Next| {
            let binding = binding.clone();
            async move { binding.authenticate(request, next).await }
        }))
        .method_not_allowed_fallback(|| async { axum::http::StatusCode::METHOD_NOT_ALLOWED })
}

#[cfg(test)]
mod tests;
