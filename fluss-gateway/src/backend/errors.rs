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

//! Classification of native `fluss-rs` failures.

use crate::backend::context::RequestContext;
use crate::error::{ErrorKind, GatewayError, Resource};
use fluss::error::{Error as FlussClientError, FlussError};

/// Logs and classifies a native failure without exposing its detail to clients.
pub(crate) fn map_fluss_error(
    what: &str,
    error: FlussClientError,
    ctx: Option<&RequestContext>,
) -> GatewayError {
    let mapped = classify_fluss_error(what, &error);
    let level = match mapped.kind() {
        ErrorKind::Backend | ErrorKind::Internal => log::Level::Error,
        ErrorKind::Unavailable | ErrorKind::Unsupported => log::Level::Warn,
        _ => log::Level::Debug,
    };
    log::log!(
        level,
        "request_id={} cluster={} operation={what:?} error={error}",
        ctx.map_or("-", RequestContext::request_id),
        ctx.map_or("-", |ctx| ctx.cluster_id().as_str()),
    );
    mapped
}

/// Classifies a native failure without logging or exposing its detail.
pub(crate) fn classify_fluss_error(what: &str, error: &FlussClientError) -> GatewayError {
    error
        .api_error()
        .and_then(|api_error| map_api_error(what, api_error))
        .unwrap_or_else(|| match error {
            FlussClientError::UnsupportedOperation { .. }
            | FlussClientError::UnsupportedVersion { .. } => GatewayError::unsupported(format!(
                "Fluss does not support the request while trying to {what}"
            )),
            FlussClientError::IllegalArgument { .. }
            | FlussClientError::RowConvertError { .. }
            | FlussClientError::ArrowError { .. } => GatewayError::invalid_argument(format!(
                "Fluss rejected the request while trying to {what}"
            )),
            FlussClientError::BufferExhausted { .. } => GatewayError::resource_exhausted(format!(
                "the Fluss write buffer is exhausted while trying to {what}"
            )),
            FlussClientError::WriterClosed { .. }
            | FlussClientError::RpcError { .. }
            | FlussClientError::WakeupError { .. } => {
                GatewayError::unavailable(format!("Fluss is unavailable while trying to {what}"))
            }
            _ if error.is_retriable() => {
                GatewayError::unavailable(format!("Fluss is unavailable while trying to {what}"))
            }
            _ => GatewayError::backend(format!("Fluss failed while trying to {what}")),
        })
}

/// Maps the protocol error codes that carry a meaning of their own. `None` falls through to the
/// transport-level classification.
fn map_api_error(what: &str, api_error: FlussError) -> Option<GatewayError> {
    Some(match api_error {
        FlussError::DatabaseNotExist => GatewayError::not_found(format!(
            "the database does not exist while trying to {what}"
        ))
        .with_resource(Resource::Database),
        FlussError::TableNotExist | FlussError::UnknownTableOrBucketException => {
            GatewayError::not_found(format!("the table does not exist while trying to {what}"))
                .with_resource(Resource::Table)
        }
        FlussError::DatabaseAlreadyExist => GatewayError::already_exists(format!(
            "the database already exists while trying to {what}"
        ))
        .with_resource(Resource::Database),
        FlussError::TableAlreadyExist => {
            GatewayError::already_exists(format!("the table already exists while trying to {what}"))
                .with_resource(Resource::Table)
        }
        // A schema is not a resource the API addresses on its own, so a missing one is reported
        // against the table it belongs to; that is the resource a caller can act on.
        FlussError::SchemaNotExist => GatewayError::not_found(format!(
            "the table schema does not exist while trying to {what}"
        ))
        .with_resource(Resource::Table),
        FlussError::PartitionNotExists => GatewayError::not_found(format!(
            "the partition does not exist while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        FlussError::PartitionAlreadyExists => GatewayError::already_exists(format!(
            "the partition already exists while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        FlussError::TableNotPartitionedException => GatewayError::invalid_argument(format!(
            "the table is not partitioned while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        FlussError::PartitionSpecInvalidException => GatewayError::invalid_argument(format!(
            "Fluss rejected the partition spec while trying to {what}"
        ))
        .with_resource(Resource::Partition),
        // The table is at its configured partition limit. A caller can act on it, by dropping a
        // partition, which is what makes it a precondition rather than a gateway capacity failure.
        FlussError::PartitionMaxNumException => GatewayError::failed_precondition(format!(
            "the table holds the maximum number of partitions, {what} refused"
        ))
        .with_resource(Resource::Partition),
        FlussError::DatabaseNotEmpty => {
            GatewayError::failed_precondition(format!("the database is not empty, {what} refused"))
                .with_resource(Resource::Database)
        }
        FlussError::InvalidDatabaseException => GatewayError::invalid_argument(format!(
            "Fluss rejected the name while trying to {what}"
        )),
        FlussError::InvalidTableException
        | FlussError::InvalidTargetColumn
        | FlussError::NonPrimaryKeyTableException => GatewayError::invalid_argument(format!(
            "Fluss rejected the table request while trying to {what}"
        ))
        .with_resource(Resource::Table),
        FlussError::InvalidConfigException => GatewayError::invalid_argument(format!(
            "the configuration is invalid while trying to {what}"
        )),
        FlussError::InvalidAlterTableException => GatewayError::invalid_argument(format!(
            "Fluss rejected the requested table alteration while trying to {what}"
        )),
        FlussError::InvalidReplicationFactor => GatewayError::invalid_argument(format!(
            "the replication factor is invalid while trying to {what}"
        )),
        FlussError::BucketMaxNumException => GatewayError::invalid_argument(format!(
            "the requested bucket count exceeds the maximum while trying to {what}"
        )),
        FlussError::RequestTimeOut => {
            GatewayError::deadline_exceeded(format!("Fluss timed out while trying to {what}"))
        }
        FlussError::RecordTooLargeException => GatewayError::limit_exceeded(format!(
            "the encoded row is too large while trying to {what}"
        )),
        FlussError::StorageBackpressureException => GatewayError::new(
            ErrorKind::StorageBackpressure,
            format!("Fluss refused the write under storage backpressure while trying to {what}"),
        ),
        FlussError::UnsupportedVersion => GatewayError::unsupported(format!(
            "Fluss does not support the request while trying to {what}"
        )),
        // TODO: Map caller authorization failures to 403 when fluss-rs supports user mode.
        FlussError::AuthenticateException => {
            GatewayError::backend(format!("Fluss rejected the gateway while trying to {what}"))
        }
        FlussError::AuthorizationException => {
            GatewayError::backend(format!("Fluss denied the gateway while trying to {what}"))
        }
        _ => return None,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::time::Duration;

    thread_local! {
        static LOGS: RefCell<Vec<(log::Level, String)>> = const { RefCell::new(Vec::new()) };
    }

    struct TestLogger;

    impl log::Log for TestLogger {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &log::Record<'_>) {
            LOGS.with_borrow_mut(|logs| logs.push((record.level(), record.args().to_string())));
        }

        fn flush(&self) {}
    }

    pub(crate) fn api_failure(error: FlussError) -> FlussClientError {
        FlussClientError::FlussAPIError {
            api_error: fluss::error::ApiError {
                code: error.code(),
                message: "server detail".to_string(),
            },
        }
    }

    #[test]
    fn native_failures_map_to_gateway_errors_and_logs() {
        log::set_logger(&TestLogger).expect("install the test logger");
        log::set_max_level(log::LevelFilter::Debug);
        let ctx = RequestContext::for_test("default", Duration::from_secs(5));
        let operation = "complete the request";
        let cases = [
            (
                api_failure(FlussError::DatabaseNotExist),
                ErrorKind::NotFound,
                "database_not_found",
            ),
            (
                api_failure(FlussError::TableNotExist),
                ErrorKind::NotFound,
                "table_not_found",
            ),
            (
                api_failure(FlussError::DatabaseAlreadyExist),
                ErrorKind::AlreadyExists,
                "database_already_exists",
            ),
            (
                api_failure(FlussError::TableAlreadyExist),
                ErrorKind::AlreadyExists,
                "table_already_exists",
            ),
            (
                api_failure(FlussError::DatabaseNotEmpty),
                ErrorKind::FailedPrecondition,
                "database_not_empty",
            ),
            (
                api_failure(FlussError::PartitionMaxNumException),
                ErrorKind::FailedPrecondition,
                "failed_precondition",
            ),
            (
                api_failure(FlussError::TableNotPartitionedException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::PartitionSpecInvalidException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidDatabaseException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidTableException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::AuthenticateException),
                ErrorKind::Backend,
                "backend",
            ),
            (
                api_failure(FlussError::AuthorizationException),
                ErrorKind::Backend,
                "backend",
            ),
            (
                api_failure(FlussError::InvalidConfigException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidAlterTableException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::InvalidReplicationFactor),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::BucketMaxNumException),
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::RequestTimeOut),
                ErrorKind::DeadlineExceeded,
                "timeout",
            ),
            (
                api_failure(FlussError::UnsupportedVersion),
                ErrorKind::Unsupported,
                "unsupported",
            ),
            (
                FlussClientError::UnsupportedVersion {
                    message: "server detail".to_string(),
                },
                ErrorKind::Unsupported,
                "unsupported",
            ),
            (
                FlussClientError::IllegalArgument {
                    message: "server detail".to_string(),
                },
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
            (
                api_failure(FlussError::NetworkException),
                ErrorKind::Unavailable,
                "unavailable",
            ),
            (
                api_failure(FlussError::NotLeaderOrFollower),
                ErrorKind::Unavailable,
                "unavailable",
            ),
            (
                FlussClientError::RowConvertError {
                    message: "server detail".to_string(),
                },
                ErrorKind::InvalidArgument,
                "invalid_argument",
            ),
        ];
        for (native, expected_kind, expected_code) in cases {
            let api_error = native.api_error();
            let rendered = native.to_string();
            let mapped = map_fluss_error(operation, native, Some(&ctx));
            assert_eq!(mapped.kind(), expected_kind, "{rendered}");
            assert_eq!(mapped.code(), expected_code, "{rendered}");
            assert!(mapped.message().contains(operation), "{}", mapped.message());
            assert!(
                !mapped.message().contains("server detail"),
                "the native detail must stay in the log: {}",
                mapped.message()
            );
            let expected_reason = match api_error {
                Some(FlussError::InvalidDatabaseException) => Some("Fluss rejected the name"),
                Some(FlussError::InvalidTableException) => Some("Fluss rejected the table request"),
                Some(FlussError::InvalidConfigException) => Some("the configuration is invalid"),
                Some(FlussError::InvalidAlterTableException) => {
                    Some("Fluss rejected the requested table alteration")
                }
                Some(FlussError::InvalidReplicationFactor) => {
                    Some("the replication factor is invalid")
                }
                Some(FlussError::BucketMaxNumException) => {
                    Some("the requested bucket count exceeds the maximum")
                }
                Some(FlussError::TableNotPartitionedException) => {
                    Some("the table is not partitioned")
                }
                Some(FlussError::PartitionSpecInvalidException) => {
                    Some("Fluss rejected the partition spec")
                }
                _ => None,
            };
            if let Some(reason) = expected_reason {
                assert_eq!(
                    mapped.message(),
                    format!("{reason} while trying to {operation}")
                );
            }
            let logs = LOGS.take();
            assert_eq!(logs.len(), 1, "{logs:?}");
            if expected_kind == ErrorKind::InvalidArgument {
                assert_eq!(logs[0].0, log::Level::Debug);
            }
            assert!(logs[0].1.contains(&rendered), "{logs:?}");
            assert!(logs[0].1.contains("request_id=test-request"), "{logs:?}");
            assert!(logs[0].1.contains("cluster=default"), "{logs:?}");
            assert!(
                logs[0].1.contains(&format!("operation={operation:?}")),
                "{logs:?}"
            );
        }
    }
}
