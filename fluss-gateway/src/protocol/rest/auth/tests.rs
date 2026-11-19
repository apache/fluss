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

use super::*;
use crate::auth::Authenticator;
use crate::backend::context::Principal;
use crate::backend::fake::FakeFlussBackend;
use crate::protocol::rest::{build_router, test_support};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, StatusCode};
use http_body_util::BodyExt;
use std::collections::BTreeMap;
use std::time::Duration;
use tower::ServiceExt;

fn config(mode: AuthenticationMode) -> SecurityConfig {
    SecurityConfig {
        authentication: mode,
        users: Some(Secret::new("alice:private-secret")),
        tokens: Some(Secret::new("private-token:alice")),
        trusted_proxy_addresses: vec!["127.0.0.1".parse().unwrap()],
        ..Default::default()
    }
}

fn app(config: &SecurityConfig) -> (Router, Arc<FakeFlussBackend>) {
    let backend = Arc::new(FakeFlussBackend::new());
    let mut state = test_support::state_with_backend(backend.clone());
    state.authentication =
        HttpAuthentication::new(config, crate::auth::build(config).unwrap()).unwrap();
    state.readiness.set_serving();
    (build_router(state, &test_support::test_options()), backend)
}

fn basic(username: &str, password: &str) -> String {
    format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    )
}

fn request(value: Option<&str>) -> Request {
    let mut builder = Request::builder().uri("/v1/clusters/default/databases");
    if let Some(value) = value {
        builder = builder.header(header::AUTHORIZATION, value);
    }
    builder.body(Body::empty()).unwrap()
}

async fn json(response: Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn trust_keeps_anonymous_requests_and_named_principals() {
    let (app, backend) = app(&config(AuthenticationMode::Trust));
    for (credential, name) in [
        (None, "anonymous"),
        (Some(basic("", "ignored")), "anonymous"),
        (Some(basic("alice", "ignored")), "alice"),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(credential.as_deref()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let contexts = backend.contexts();
        let principal = contexts.last().unwrap().principal().unwrap();
        assert_eq!(principal.name(), name);
    }
    for name in ["anonymous", " anonymous "] {
        assert_eq!(
            app.clone()
                .oneshot(request(Some(&basic(name, "ignored"))))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

#[tokio::test]
async fn password_and_token_reject_before_backend_and_issue_challenges() {
    for (mode, good, challenge) in [
        (
            AuthenticationMode::Password,
            basic("alice", "private-secret"),
            "Basic",
        ),
        (
            AuthenticationMode::Token,
            "Bearer private-token".into(),
            "Bearer",
        ),
    ] {
        let (app, backend) = app(&config(mode));
        for value in [None, Some("invalid"), Some("Bearer wrong")] {
            let response = app.clone().oneshot(request(value)).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(
                response.headers()[header::WWW_AUTHENTICATE]
                    .to_str()
                    .unwrap()
                    .starts_with(challenge)
            );
            let body = json(response).await;
            assert_eq!(body["error"]["code"], "unauthenticated");
            assert!(!body.to_string().contains("private-secret"));
            assert!(!body.to_string().contains("private-token"));
        }
        assert!(backend.contexts().is_empty());
        assert_eq!(
            app.oneshot(request(Some(&good))).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(backend.contexts()[0].principal().unwrap().name(), "alice");
    }
}

#[tokio::test]
async fn malformed_basic_and_duplicate_authorization_do_not_fall_back_to_anonymous() {
    let (app, backend) = app(&config(AuthenticationMode::Trust));
    for value in [
        "Basic !!!".into(),
        format!("Basic {}", STANDARD.encode("no-colon")),
        basic(&"x".repeat(257), "x"),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(Some(&value)))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let mut duplicate = request(Some(&basic("alice", "ignored")));
    duplicate.headers_mut().append(
        header::AUTHORIZATION,
        HeaderValue::from_static("Basic Yjpj"),
    );
    assert_eq!(
        app.oneshot(duplicate).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(backend.contexts().is_empty());
}

#[tokio::test]
async fn trusted_header_requires_the_socket_peer_and_a_unique_identity() {
    let (app, backend) = app(&config(AuthenticationMode::TrustedHeader));
    for (peer, name, status) in [
        (None, Some("alice"), StatusCode::FORBIDDEN),
        (Some("192.0.2.1:12"), Some("alice"), StatusCode::FORBIDDEN),
        (Some("127.0.0.1:12"), None, StatusCode::FORBIDDEN),
        (
            Some("127.0.0.1:12"),
            Some("alice,bob"),
            StatusCode::FORBIDDEN,
        ),
        (Some("127.0.0.1:12"), Some("alice"), StatusCode::OK),
        (Some("[::ffff:127.0.0.1]:12"), Some("bob"), StatusCode::OK),
        (
            Some("127.0.0.1:12"),
            Some("anonymous"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let mut req = request(Some(&basic("fallback", "ignored")));
        if let Some(peer) = peer {
            req.extensions_mut()
                .insert(peer.parse::<SocketAddr>().unwrap());
        }
        if let Some(name) = name {
            req.headers_mut()
                .insert("x-forwarded-user", HeaderValue::from_str(name).unwrap());
        }
        req.headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_static("127.0.0.1"));
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), status);
    }
    for duplicate in [false, true] {
        let mut req = request(None);
        req.extensions_mut()
            .insert("127.0.0.1:12".parse::<SocketAddr>().unwrap());
        req.headers_mut()
            .insert("x-forwarded-user", HeaderValue::from_static("alice"));
        if duplicate {
            req.headers_mut()
                .append("x-forwarded-user", HeaderValue::from_static("bob"));
        } else {
            req.headers_mut().insert(
                header::CONNECTION,
                HeaderValue::from_static("X-Forwarded-User"),
            );
        }
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(backend.contexts().len(), 2);
}

#[tokio::test]
async fn protected_routes_require_authentication_but_probes_do_not() {
    let backend = Arc::new(FakeFlussBackend::new());
    let mut state = test_support::state_with_backend(backend.clone());
    let config = config(AuthenticationMode::Password);
    state.authentication =
        HttpAuthentication::new(&config, crate::auth::build(&config).unwrap()).unwrap();
    state.readiness.set_serving();
    let document = state.openapi.clone();
    let app = build_router(state, &test_support::test_options());
    for (path, item) in document.get().unwrap()["paths"].as_object().unwrap() {
        let uri = path
            .split('/')
            .map(|part| {
                if part == "{cluster}" {
                    "default"
                } else if part.starts_with('{') {
                    "fixture"
                } else {
                    part
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        for method in item
            .as_object()
            .unwrap()
            .keys()
            .filter(|m| ["get", "post", "delete", "patch", "put"].contains(&m.as_str()))
        {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::from_bytes(method.to_uppercase().as_bytes()).unwrap())
                        .uri(&uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let expected = if path == "/health" || path == "/ready" {
                200
            } else {
                401
            };
            assert_eq!(response.status().as_u16(), expected, "{method} {path}");
        }
    }
    assert!(backend.contexts().is_empty());
}

#[test]
fn config_rejects_insecure_password_transport_and_bad_proxy_settings() {
    let mut gateway = crate::config::GatewayConfig {
        security: config(AuthenticationMode::Password),
        ..Default::default()
    };
    gateway.server.rest.bind_address = "[::ffff:127.0.0.1]:0".parse().unwrap();
    assert!(gateway.validate().is_ok());
    assert!(validate_config(&config(AuthenticationMode::Password), false).is_err());
    assert!(validate_config(&config(AuthenticationMode::Token), true).is_ok());
    assert!(
        validate_config(
            &SecurityConfig {
                allow_insecure_transport: true,
                ..config(AuthenticationMode::Password)
            },
            false
        )
        .is_ok()
    );
    assert!(
        validate_config(
            &SecurityConfig {
                trusted_proxy_addresses: vec![],
                ..config(AuthenticationMode::TrustedHeader)
            },
            true
        )
        .is_err()
    );
    for name in ["bad name", "Authorization", "Cookie", "Connection"] {
        assert!(
            validate_config(
                &SecurityConfig {
                    trusted_header_name: Some(name.into()),
                    ..config(AuthenticationMode::TrustedHeader)
                },
                true
            )
            .is_err()
        );
    }
}

struct TestAuthenticator;

#[async_trait]
impl Authenticator for TestAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        let Credential::Bearer(token) = credential else {
            unreachable!()
        };
        match token.expose() {
            "reject" => Err(AuthenticationError::Unauthenticated),
            "forbidden" => Err(AuthenticationError::Forbidden),
            "busy" => Err(AuthenticationError::ResourceExhausted),
            "fail" => Err(AuthenticationError::Internal),
            "wait" => std::future::pending().await,
            _ => Ok(Principal::new("plugin-user", BTreeMap::new())),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn injected_authenticator_uses_the_same_router_and_deadline() {
    let backend = Arc::new(FakeFlussBackend::new());
    let mut state = test_support::state_with_backend(backend.clone());
    state.authentication = HttpAuthentication::new(
        &config(AuthenticationMode::Token),
        Arc::new(TestAuthenticator),
    )
    .unwrap();
    state.readiness.set_serving();
    let mut options = test_support::test_options();
    options.request_timeout = Duration::from_millis(30);
    let app = build_router(state, &options);
    for (token, status) in [
        ("ok", 200),
        ("reject", 401),
        ("forbidden", 403),
        ("busy", 429),
        ("fail", 500),
        ("wait", 504),
    ] {
        let response = app
            .clone()
            .oneshot(request(Some(&format!("Bearer {token}"))))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        if status == 429 {
            assert!(response.headers().contains_key(header::RETRY_AFTER));
        }
    }
    assert_eq!(backend.contexts().len(), 1);
    assert_eq!(
        backend.contexts()[0].principal().unwrap().name(),
        "plugin-user"
    );
}

#[test]
fn openapi_declares_the_selected_mode() {
    for (mode, scheme, status) in [
        (AuthenticationMode::Trust, "basicAuth", "401"),
        (AuthenticationMode::Password, "basicAuth", "401"),
        (AuthenticationMode::Token, "bearerAuth", "401"),
        (AuthenticationMode::TrustedHeader, "trustedProxy", "403"),
    ] {
        let config = config(mode);
        let mut state = test_support::test_state();
        state.authentication =
            HttpAuthentication::new(&config, crate::auth::build(&config).unwrap()).unwrap();
        let document = state.openapi.clone();
        let _app = build_router(state, &test_support::test_options());
        let doc = document.get().unwrap();
        for path in ["/health", "/ready"] {
            assert_eq!(doc["paths"][path]["get"]["security"], json!([]));
        }
        assert!(doc["components"]["securitySchemes"][scheme].is_object());
        assert_eq!(
            doc["security"],
            if mode == AuthenticationMode::Trust {
                json!([{}, {scheme: []}])
            } else {
                json!([{scheme: []}])
            }
        );
        assert_eq!(
            doc["paths"]["/v1/clusters"]["get"]["responses"][status]["$ref"],
            "#/components/responses/AuthenticationFailed"
        );
        assert_eq!(
            doc["components"]["responses"]["AuthenticationFailed"]["headers"]["WWW-Authenticate"]
                .is_object(),
            status == "401"
        );
    }
}
