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

use crate::backend::context::Principal;
use crate::config::{AuthenticationMode, Secret, SecurityConfig};
use async_trait::async_trait;
use ring::digest;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use subtle::{Choice, ConditionallySelectable, ConstantTimeEq};
use tokio::sync::Semaphore;

pub(crate) const MAX_CREDENTIAL_BYTES: usize = 8 * 1024;
const MAX_PRINCIPAL_BYTES: usize = 256;

pub enum Credential {
    Trust { username: Option<String> },
    Password { username: String, password: Secret },
    Bearer(Secret),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationError {
    Unauthenticated,
    Forbidden,
    ResourceExhausted,
    Internal,
}

#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError>;
}

pub(crate) fn validate_config(config: &SecurityConfig) -> Result<(), String> {
    match config.authentication {
        AuthenticationMode::Password => {
            parse_users(config.users.as_ref().map_or("", Secret::expose))
                .map_err(|error| format!("gateway.security.users: {error}"))?;
        }
        AuthenticationMode::Token => {
            parse_tokens(config.tokens.as_ref().map_or("", Secret::expose))
                .map_err(|error| format!("gateway.security.tokens: {error}"))?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn build(config: &SecurityConfig) -> Result<Arc<dyn Authenticator>, String> {
    Ok(match config.authentication {
        AuthenticationMode::Trust | AuthenticationMode::TrustedHeader => Arc::new(Trust),
        AuthenticationMode::Password => Arc::new(Password::new(parse_users(
            config.users.as_ref().map_or("", Secret::expose),
        )?)),
        AuthenticationMode::Token => Arc::new(Token(parse_tokens(
            config.tokens.as_ref().map_or("", Secret::expose),
        )?)),
    })
}

pub(crate) fn valid_principal(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty()
        && trimmed != "anonymous"
        && name.len() <= MAX_PRINCIPAL_BYTES
        && !name.chars().any(char::is_control)
}

const MAX_STORE_ENTRIES: usize = 1024;
const MAX_BCRYPT_COST: u32 = 14;

#[derive(Clone)]
enum PasswordRecord {
    Plain([u8; 32]),
    Bcrypt(Secret),
}

type UserStore = BTreeMap<String, PasswordRecord>;
type TokenStore = Vec<([u8; 32], String)>;

fn sha256(value: &[u8]) -> [u8; 32] {
    digest::digest(&digest::SHA256, value)
        .as_ref()
        .try_into()
        .expect("SHA-256 length")
}

fn parse_users(raw: &str) -> Result<UserStore, String> {
    let mut users = BTreeMap::new();
    for (position, entry) in raw.split(',').enumerate() {
        let entry = entry.trim_start();
        if entry.is_empty() {
            continue;
        }
        let error = |reason| format!("user entry {position}: {reason}");
        let (name, value) = entry
            .split_once(':')
            .ok_or_else(|| error("expected principal:secret or principal:bcrypt:<hash>"))?;
        let name = name.trim();
        if !valid_principal(name) {
            return Err(error("invalid principal"));
        }
        if value.is_empty() || value.len() > MAX_CREDENTIAL_BYTES {
            return Err(error("secret must contain 1..=8192 bytes"));
        }
        let record = if let Some(hash) = value.strip_prefix("bcrypt:") {
            let parts = hash
                .parse::<bcrypt::HashParts>()
                .map_err(|_| error("malformed bcrypt hash"))?;
            if !(4..=MAX_BCRYPT_COST).contains(&parts.get_cost()) {
                return Err(error("bcrypt cost must be between 4 and 14"));
            }
            PasswordRecord::Bcrypt(Secret::new(hash))
        } else {
            PasswordRecord::Plain(sha256(value.as_bytes()))
        };
        if users.insert(name.to_owned(), record).is_some() {
            return Err(error("duplicate principal"));
        }
        if users.len() > MAX_STORE_ENTRIES {
            return Err(error("too many users (maximum 1024)"));
        }
    }
    if users.is_empty() {
        return Err("must configure at least one user".into());
    }
    Ok(users)
}

fn parse_tokens(raw: &str) -> Result<TokenStore, String> {
    let mut tokens = Vec::new();
    let mut seen = BTreeSet::new();
    for (position, entry) in raw.split(',').enumerate() {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let error = |reason| format!("token entry {position}: {reason}");
        let (value, name) = entry
            .rsplit_once(':')
            .ok_or_else(|| error("expected token:principal or sha256:<hex>:principal"))?;
        let name = name.trim();
        if !valid_principal(name) {
            return Err(error("invalid principal"));
        }
        let digest = if let Some(hex) = value.strip_prefix("sha256:") {
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(error("malformed sha256 digest"));
            }
            let mut bytes = [0u8; 32];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
                    .expect("validated SHA-256 digest");
            }
            bytes
        } else {
            if !valid_token(value) {
                return Err(error("invalid bearer token"));
            }
            sha256(value.as_bytes())
        };
        if !seen.insert(digest) {
            return Err(error("duplicate token digest"));
        }
        tokens.push((digest, name.to_owned()));
        if tokens.len() > MAX_STORE_ENTRIES {
            return Err(error("too many tokens (maximum 1024)"));
        }
    }
    if tokens.is_empty() {
        return Err("must configure at least one token".into());
    }
    Ok(tokens)
}

pub(crate) fn valid_token(value: &str) -> bool {
    let token = value.trim_end_matches('=');
    !token.is_empty()
        && value.len() <= MAX_CREDENTIAL_BYTES
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(&b))
}

fn named(name: String) -> Principal {
    Principal::new(name, BTreeMap::new())
}

struct Trust;

#[async_trait]
impl Authenticator for Trust {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        let Credential::Trust { username } = credential else {
            return Err(AuthenticationError::Unauthenticated);
        };
        let username = username.unwrap_or_default();
        if username.is_empty() {
            Ok(named("anonymous".into()))
        } else if valid_principal(&username) {
            Ok(named(username))
        } else {
            Err(AuthenticationError::Unauthenticated)
        }
    }
}

struct Password {
    users: UserStore,
    dummy: Option<Secret>,
    permits: Arc<Semaphore>,
}

impl Password {
    fn new(users: UserStore) -> Self {
        let dummy = users
            .values()
            .filter_map(|record| match record {
                PasswordRecord::Bcrypt(hash) => Some(hash),
                _ => None,
            })
            .max_by_key(|hash| {
                hash.expose()
                    .parse::<bcrypt::HashParts>()
                    .expect("validated bcrypt hash")
                    .get_cost()
            })
            .cloned();
        Self {
            users,
            dummy,
            permits: Arc::new(Semaphore::new(4)),
        }
    }
}

#[async_trait]
impl Authenticator for Password {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        let Credential::Password { username, password } = credential else {
            return Err(AuthenticationError::Unauthenticated);
        };
        let record = self.users.get(&username);
        let exists = record.is_some();
        let record = record.cloned().unwrap_or_else(|| {
            self.dummy
                .clone()
                .map_or(PasswordRecord::Plain([0; 32]), PasswordRecord::Bcrypt)
        });
        let valid = match record {
            PasswordRecord::Plain(expected) => {
                bool::from(sha256(password.expose().as_bytes()).ct_eq(&expected))
            }
            PasswordRecord::Bcrypt(hash) => {
                if password.expose().len() > 72 {
                    return Err(AuthenticationError::Unauthenticated);
                }
                let permit = self
                    .permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| AuthenticationError::ResourceExhausted)?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    bcrypt::verify(password.expose(), hash.expose())
                        .map_err(|_| AuthenticationError::Internal)
                })
                .await
                .map_err(|_| AuthenticationError::Internal)??
            }
        };
        if valid && exists {
            Ok(named(username))
        } else {
            Err(AuthenticationError::Unauthenticated)
        }
    }
}

struct Token(TokenStore);

#[async_trait]
impl Authenticator for Token {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        let Credential::Bearer(token) = credential else {
            return Err(AuthenticationError::Unauthenticated);
        };
        let digest = sha256(token.expose().as_bytes());
        let mut found = Choice::from(0);
        let mut index = 0u64;
        for (position, (expected, _)) in self.0.iter().enumerate() {
            let equal = digest.ct_eq(expected);
            index = u64::conditional_select(&index, &(position as u64), equal);
            found |= equal;
        }
        if bool::from(found) {
            Ok(named(self.0[index as usize].1.clone()))
        } else {
            Err(AuthenticationError::Unauthenticated)
        }
    }
}

#[cfg(test)]
mod tests;
