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

fn password(name: &str, value: &str) -> Credential {
    Credential::Password {
        username: name.into(),
        password: Secret::new(value),
    }
}

#[tokio::test]
async fn password_verifies_plaintext_and_bcrypt() {
    let hash = bcrypt::hash("sensitive-password", 4).unwrap();
    let authenticator = build(&SecurityConfig {
        authentication: AuthenticationMode::Password,
        users: Some(Secret::new(format!("alice:plain-secret,bob:bcrypt:{hash}"))),
        ..Default::default()
    })
    .unwrap();
    for (name, secret) in [("alice", "plain-secret"), ("bob", "sensitive-password")] {
        assert_eq!(
            authenticator
                .authenticate(password(name, secret))
                .await
                .unwrap()
                .name(),
            name
        );
    }
    for (name, secret) in [
        ("alice", "wrong"),
        ("bob", "wrong"),
        ("unknown", "sensitive-password"),
    ] {
        assert_eq!(
            authenticator.authenticate(password(name, secret)).await,
            Err(AuthenticationError::Unauthenticated)
        );
    }
    assert_eq!(
        authenticator
            .authenticate(password("bob", &"x".repeat(73)))
            .await,
        Err(AuthenticationError::Unauthenticated)
    );
}

#[test]
fn cancelled_bcrypt_verification_retains_its_capacity_until_finished() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, wait) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
        ready.await.unwrap();

        let hash = bcrypt::hash("secret", 4).unwrap();
        let authenticator = Password::new(parse_users(&format!("alice:bcrypt:{hash}")).unwrap());
        let _reserved = authenticator.permits.acquire_many(3).await.unwrap();
        let mut verification = Box::pin(authenticator.authenticate(password("alice", "secret")));
        assert!(futures_util::poll!(&mut verification).is_pending());
        assert_eq!(
            authenticator
                .authenticate(password("alice", "secret"))
                .await,
            Err(AuthenticationError::ResourceExhausted)
        );
        drop(verification);
        assert_eq!(authenticator.permits.available_permits(), 0);

        release.send(()).unwrap();
        blocker.await.unwrap();
        let _finished = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            authenticator.permits.acquire(),
        )
        .await
        .unwrap()
        .unwrap();
    });
}

#[tokio::test]
async fn token_resolves_plaintext_and_digest_entries() {
    let digest: String = sha256(b"middle")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let authenticator = build(&SecurityConfig {
        authentication: AuthenticationMode::Token,
        tokens: Some(Secret::new(format!("first:alice,sha256:{digest}:bob"))),
        ..Default::default()
    })
    .unwrap();
    for (value, name) in [("first", "alice"), ("middle", "bob")] {
        assert_eq!(
            authenticator
                .authenticate(Credential::Bearer(Secret::new(value)))
                .await
                .unwrap()
                .name(),
            name
        );
    }
    assert_eq!(
        authenticator
            .authenticate(Credential::Bearer(Secret::new("wrong")))
            .await,
        Err(AuthenticationError::Unauthenticated)
    );
}

#[test]
fn stores_reject_duplicate_and_malformed_entries_without_exposing_secrets() {
    for raw in [
        "",
        "alice",
        ":private-secret",
        "alice:private-secret,alice:other",
        "alice:bcrypt:private-secret",
        "anonymous:private-secret",
    ] {
        let error = parse_users(raw).err().unwrap();
        assert!(!error.contains("private-secret"));
    }
    for raw in [
        "",
        "private-secret",
        "private-secret:",
        "private-secret:alice,private-secret:bob",
        "private-secret:anonymous",
        "sha256:private-secret:alice",
    ] {
        let error = parse_tokens(raw).unwrap_err();
        assert!(!error.contains("private-secret"));
    }
}
