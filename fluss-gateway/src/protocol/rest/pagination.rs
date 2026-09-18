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

//! Keyset pagination for catalog list endpoints.
//!
//! The page token is self-describing: it carries the cluster and collection it belongs to, its scope,
//! and the last name of the page it follows, base64url-encoded. Nothing is stored server-side, so any
//! instance can serve any page, and keying on the last name rather than on an offset means concurrent
//! DDL cannot make a page skip or repeat an entry.
//!
//! Every part of a token's scope is validated on decode. The cluster is part of it: a cursor from one
//! cluster replayed against another would answer with a page silently missing every entry up to that
//! cursor, which a caller cannot detect.
//!
//! Fluss has no server-side pagination, so a page is cut from the full list of names. That is
//! acceptable at catalog scale and is replaced the day the RPC offers a cursor.

use crate::error::{GatewayError, GatewayResult};
use axum::http::Uri;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Default page size.
const DEFAULT_MAX_RESULTS: usize = 100;
/// Maximum page size, bounding one response body.
const MAX_MAX_RESULTS: usize = 1000;
/// Current token layout. A token from another version is rejected, never reinterpreted.
const TOKEN_VERSION: u32 = 1;

/// The paginated collection a token belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collection {
    Databases,
    Tables,
    Partitions,
}

impl Collection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Databases => "databases",
            Self::Tables => "tables",
            Self::Partitions => "partitions",
        }
    }
}

/// One validated page request: how many entries, and which name to continue after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    max_results: usize,
    after: Option<String>,
    /// The cluster this page belongs to, which the token has to agree with.
    cluster: String,
    collection: Collection,
    /// The parent resource of a scoped collection, which the token has to agree with.
    scope: Option<String>,
}

impl Page {
    /// Validates the query string of a list request.
    ///
    /// Called before the request reaches the backend, so a malformed token is always 400 and never
    /// turns into the 404 of a parent resource that happens not to exist.
    pub fn parse(
        uri: &Uri,
        cluster: &str,
        collection: Collection,
        scope: Option<&str>,
    ) -> GatewayResult<Self> {
        let mut max_results = DEFAULT_MAX_RESULTS;
        let mut after = None;
        for (name, value) in query_pairs(uri)? {
            match name {
                "max_results" => {
                    max_results = value
                        .parse()
                        .ok()
                        .filter(|value| (1..=MAX_MAX_RESULTS).contains(value))
                        .ok_or_else(|| {
                            GatewayError::invalid_argument(format!(
                                "max_results must be between 1 and {MAX_MAX_RESULTS}"
                            ))
                        })?;
                }
                "page_token" => {
                    after = Some(decode_token(value, cluster, collection, scope)?);
                }
                _ => {
                    return Err(GatewayError::invalid_argument(format!(
                        "unsupported query parameter `{name}`"
                    )));
                }
            }
        }
        Ok(Self {
            max_results,
            after,
            cluster: cluster.to_string(),
            collection,
            scope: scope.map(str::to_string),
        })
    }

    /// Cuts one page out of `names` and renders the token of the next one.
    ///
    /// The gateway sorts by UTF-8 byte order, because Fluss does not promise an order and keyset
    /// pagination needs one.
    pub fn apply(&self, names: Vec<String>) -> (Vec<String>, Option<String>) {
        self.apply_by(names, |name| Cow::Borrowed(name))
    }

    /// Paginates entries by a borrowed or computed name.
    pub fn apply_by<T>(
        &self,
        mut entries: Vec<T>,
        key: impl Fn(&T) -> Cow<'_, str>,
    ) -> (Vec<T>, Option<String>) {
        entries.sort_unstable_by(|left, right| key(left).cmp(&key(right)));
        let start = match &self.after {
            Some(after) => entries.partition_point(|entry| key(entry).as_ref() <= after.as_str()),
            None => 0,
        };
        let has_more = entries.len() - start > self.max_results;
        let page: Vec<T> = entries
            .into_iter()
            .skip(start)
            .take(self.max_results)
            .collect();
        let next = page.last().filter(|_| has_more).map(|last| {
            encode_token(
                &self.cluster,
                self.collection,
                self.scope.as_deref(),
                &key(last),
            )
        });
        (page, next)
    }
}

/// The token payload. Compact field names keep the encoded token short.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenPayload<'a> {
    v: u32,
    #[serde(borrow)]
    c: Cow<'a, str>,
    #[serde(borrow)]
    k: Cow<'a, str>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    db: Option<Cow<'a, str>>,
    #[serde(borrow)]
    after: Cow<'a, str>,
}

fn encode_token(cluster: &str, collection: Collection, scope: Option<&str>, after: &str) -> String {
    let payload = TokenPayload {
        v: TOKEN_VERSION,
        c: Cow::Borrowed(cluster),
        k: Cow::Borrowed(collection.as_str()),
        db: scope.map(Cow::Borrowed),
        after: Cow::Borrowed(after),
    };
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).expect("the page token is serializable"))
}

/// Decodes a token and refuses one minted for another cluster or endpoint, rather than reinterpreting
/// it against the collection at hand.
fn decode_token(
    token: &str,
    cluster: &str,
    collection: Collection,
    scope: Option<&str>,
) -> GatewayResult<String> {
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| bad_token())?;
    let payload: TokenPayload<'_> = serde_json::from_slice(&bytes).map_err(|_| bad_token())?;
    if payload.v != TOKEN_VERSION
        || payload.c != cluster
        || payload.k != collection.as_str()
        || payload.db.as_deref() != scope
    {
        return Err(bad_token());
    }
    Ok(payload.after.into_owned())
}

fn bad_token() -> GatewayError {
    GatewayError::invalid_argument("page_token is not a token this endpoint issued")
}

/// Splits the query string into its pairs, rejecting a repeated or valueless parameter.
///
/// Parsed here rather than with a form decoder: every parameter the gateway defines is either a number
/// or a base64url token, so no percent-decoding is involved, and a strict reading turns a typo into a
/// 400 instead of a silently ignored parameter.
fn query_pairs(uri: &Uri) -> GatewayResult<Vec<(&str, &str)>> {
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for pair in uri.query().unwrap_or_default().split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair.split_once('=').ok_or_else(|| {
            GatewayError::invalid_argument(format!("query parameter `{pair}` has no value"))
        })?;
        if pairs.iter().any(|(seen, _)| *seen == name) {
            return Err(GatewayError::invalid_argument(format!(
                "query parameter `{name}` is repeated"
            )));
        }
        pairs.push((name, value));
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn uri(query: &str) -> Uri {
        format!("/v1/clusters/default/databases?{query}")
            .parse()
            .expect("valid test URI")
    }

    fn names(count: usize) -> Vec<String> {
        (0..count).map(|index| format!("name_{index:03}")).collect()
    }

    /// Walking every page returns each name exactly once, in order, and the last page carries no token.
    #[test]
    fn a_token_walks_the_collection_once_and_in_order() {
        let mut collected: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..10 {
            let query = match &token {
                Some(token) => format!("max_results=3&page_token={token}"),
                None => "max_results=3".to_string(),
            };
            let page = Page::parse(&uri(&query), "default", Collection::Databases, None).unwrap();
            let (entries, next) = page.apply(names(7));
            collected.extend(entries);
            token = next;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(collected, names(7));
        assert_eq!(token, None, "the last page carries no token");
    }

    /// A page cut from a collection that changed keeps its place: keyset pagination continues after the
    /// last name returned, so an insert before it cannot repeat an entry and a delete cannot skip one.
    #[test]
    fn concurrent_changes_neither_repeat_nor_skip_an_entry() {
        let (first, token) = Page::parse(
            &uri("max_results=2"),
            "default",
            Collection::Databases,
            None,
        )
        .unwrap()
        .apply(vec!["b".into(), "d".into(), "f".into()]);
        assert_eq!(first, ["b", "d"]);

        let query = format!("max_results=2&page_token={}", token.expect("a next token"));
        let (second, _) = Page::parse(&uri(&query), "default", Collection::Databases, None)
            .unwrap()
            // `a` was created and `d` dropped while the caller paged.
            .apply(vec!["a".into(), "b".into(), "f".into()]);
        assert_eq!(second, ["f"]);
    }

    /// A token only continues the endpoint that issued it, on the cluster that issued it.
    #[test]
    fn a_token_from_another_cluster_endpoint_or_version_is_refused() {
        let token = encode_token("alpha", Collection::Tables, Some("sales"), "orders");
        for (cluster, collection, scope) in [
            ("zeta", Collection::Tables, Some("sales")),
            ("alpha", Collection::Databases, None),
            ("alpha", Collection::Tables, None),
            ("alpha", Collection::Tables, Some("other")),
        ] {
            let error = decode_token(&token, cluster, collection, scope).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{cluster}");
        }
        assert_eq!(
            decode_token(&token, "alpha", Collection::Tables, Some("sales")).unwrap(),
            "orders"
        );

        // A future layout is rejected instead of being read with today's meaning.
        let future =
            URL_SAFE_NO_PAD.encode(br#"{"v":2,"c":"default","k":"databases","after":"a"}"#);
        assert!(decode_token(&future, "default", Collection::Databases, None).is_err());
    }

    #[test]
    fn a_malformed_token_is_rejected() {
        for token in [
            "not-base64!!",
            // Valid base64 that is not the payload.
            &URL_SAFE_NO_PAD.encode("{}"),
            // The cluster the payload has to carry is missing.
            &URL_SAFE_NO_PAD.encode(br#"{"v":1,"k":"databases","after":"a"}"#),
            &URL_SAFE_NO_PAD
                .encode(br#"{"v":1,"c":"default","k":"databases","after":"a","extra":1}"#),
            "",
        ] {
            assert!(
                decode_token(token, "default", Collection::Databases, None).is_err(),
                "accepted {token:?}"
            );
        }
    }

    /// Page-size bounds and unsupported query parameters.
    #[test]
    fn the_page_size_is_bounded_and_unknown_parameters_are_refused() {
        let parse = |query: &str| Page::parse(&uri(query), "default", Collection::Databases, None);
        assert_eq!(parse("").unwrap().max_results, DEFAULT_MAX_RESULTS);
        assert_eq!(parse("max_results=1").unwrap().max_results, 1);
        assert_eq!(
            parse(&format!("max_results={MAX_MAX_RESULTS}"))
                .unwrap()
                .max_results,
            MAX_MAX_RESULTS
        );
        for rejected in [
            "max_results=0",
            "max_results=1001",
            "max_results=-1",
            "max_results=all",
            "max_results=",
            "max_results",
            "max_results=1&max_results=2",
            "maxResults=10",
            "limit=10",
        ] {
            let error = parse(rejected)
                .err()
                .unwrap_or_else(|| panic!("{rejected}"));
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{rejected}");
        }
    }
}
