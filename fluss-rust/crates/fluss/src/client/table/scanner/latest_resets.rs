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
use crate::client::consumer_offsets::invalid;
use futures::FutureExt;
use std::task::Poll;

#[derive(Default)]
pub(super) struct LatestResets {
    queued: HashSet<TableBucket>,
    requests: Vec<ResetRequest>,
}

struct ResetRequest {
    buckets: Vec<TableBucket>,
    lookup: JoinHandle<Result<Option<HashMap<i32, i64>>>>,
}

impl Drop for ResetRequest {
    fn drop(&mut self) {
        self.lookup.abort();
    }
}

impl LatestResets {
    pub(super) fn queue(&mut self, bucket: TableBucket) {
        self.queued.insert(bucket);
    }

    pub(super) fn forget(&mut self, bucket: &TableBucket) {
        self.queued.remove(bucket);
        self.requests.retain_mut(|request| {
            request.buckets.retain(|b| b != bucket);
            !request.buckets.is_empty()
        });
    }

    fn start(&mut self, scanner: &LogScannerInner) {
        if self.queued.is_empty() {
            return;
        }
        let cluster = scanner.metadata.get_cluster();
        let mut groups = HashMap::<_, Vec<_>>::new();
        for bucket in self.queued.drain() {
            groups
                .entry((
                    bucket.partition_id(),
                    cluster.leader_for(&bucket).map(|n| n.id()),
                ))
                .or_default()
                .push(bucket);
        }
        for ((partition, leader), buckets) in groups {
            let metadata = scanner.metadata.clone();
            let connections = scanner.log_fetcher.conns.clone();
            let path = scanner.table_path.clone();
            let first = &buckets[0];
            let table_id = first.table_id();
            let node = leader.and_then(|id| cluster.get_tablet_server(id)).cloned();
            let request = message::ListOffsetsRequest::new(
                table_id,
                partition,
                buckets.iter().map(TableBucket::bucket_id).collect(),
                message::OffsetSpec::Latest,
            )
            .with_routing_bucket_count(cluster.bucket_count(first.table_or_partition()));
            let lookup = tokio::spawn(async move {
                if let Some(node) = node {
                    let call = async {
                        let conn = connections.get_connection(&node).await?;
                        conn.request(request).await?.offsets()
                    };
                    match tokio::time::timeout(Duration::from_secs(30), call).await {
                        Ok(Ok(offsets)) => return Ok(Some(offsets)),
                        Ok(Err(err)) if !err.is_retriable() => return Err(err),
                        _ => (),
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Err(e) = metadata
                    .update_tables_metadata(
                        &HashSet::from([&path]),
                        &HashSet::new(),
                        partition.into_iter().collect(),
                    )
                    .await
                    && !e.is_retriable()
                {
                    return Err(e);
                }
                if metadata
                    .get_cluster()
                    .get_table_id(&path)
                    .is_some_and(|id| id != table_id)
                {
                    return Err(invalid("Table was recreated during offset reset"));
                }
                Ok(None) // Regroup using refreshed leaders on the next poll.
            });
            self.requests.push(ResetRequest { buckets, lookup });
        }
    }
}

impl LogScannerInner {
    async fn complete_next_reset(&self) -> Result<()> {
        let offsets = std::future::poll_fn(|cx| {
            let mut resets = self.latest_resets.lock();
            for i in 0..resets.requests.len() {
                if let Poll::Ready(result) =
                    std::pin::Pin::new(&mut resets.requests[i].lookup).poll(cx)
                {
                    let mut request = resets.requests.swap_remove(i);
                    let buckets = std::mem::take(&mut request.buckets);
                    let values = match result.map_err(|e| invalid(&e.to_string())).and_then(|v| v) {
                        Ok(Some(values)) => values,
                        other => {
                            resets.queued.extend(buckets);
                            return Poll::Ready(other.map(|_| HashMap::new()));
                        }
                    };
                    let result: Result<HashMap<_, _>> = buckets
                        .iter()
                        .map(|b| {
                            values
                                .get(&b.bucket_id())
                                .copied()
                                .filter(|v| *v >= 0)
                                .map(|v| (b.clone(), v))
                                .ok_or_else(|| {
                                    invalid("Missing latest offset for requested bucket")
                                })
                        })
                        .collect();
                    if result.is_err() {
                        resets.queued.extend(buckets);
                    }
                    return Poll::Ready(result);
                }
            }
            Poll::Pending
        })
        .await?;
        if !offsets.is_empty() {
            self.assign_buckets_at_offsets(offsets);
        }
        Ok(())
    }

    pub(super) fn drive_latest_resets(&self) -> Result<()> {
        let mut resets = self.latest_resets.lock();
        resets.start(self);
        if resets.requests.is_empty() {
            return Ok(());
        }
        drop(resets);
        while let Some(result) = self.complete_next_reset().now_or_never() {
            result?;
        }
        self.latest_resets.lock().start(self);
        Ok(())
    }

    pub(super) async fn wait_for_data_or_reset(&self, remaining: Duration) -> Result<bool> {
        let data = self.log_fetcher.log_fetch_buffer.await_not_empty(remaining);
        if self.latest_resets.lock().requests.is_empty() {
            return data.await;
        }
        tokio::select! {
            ready = data => ready,
            result = self.complete_next_reset() => result.map(|()| true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_leader_does_not_block_another_reset_or_override_a_seek() -> Result<()> {
        let (scanner, _, _) = super::super::tests::scanner_fixture(false)?;
        let slow = TableBucket::new(1, 0);
        let fast = TableBucket::new(1, 1);
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let mut requests = vec![
            ResetRequest {
                buckets: vec![slow.clone()],
                lookup: tokio::spawn(async {
                    let _ = wait.await;
                    Ok(Some(HashMap::from([(0, 99)])))
                }),
            },
            ResetRequest {
                buckets: vec![fast.clone()],
                lookup: tokio::spawn(async { Ok(Some(HashMap::from([(1, 7)]))) }),
            },
        ];
        scanner.latest_resets.lock().requests.append(&mut requests);
        scanner.complete_next_reset().await?;
        assert_eq!(scanner.log_scanner_status.get_bucket_offset(&fast), Some(7));
        scanner.assign_buckets_at_offsets(HashMap::from([(slow.clone(), 42)]));
        let _ = release.send(());
        assert!(scanner.latest_resets.lock().requests.is_empty());
        assert_eq!(
            scanner.log_scanner_status.get_bucket_offset(&slow),
            Some(42)
        );
        Ok(())
    }
}
