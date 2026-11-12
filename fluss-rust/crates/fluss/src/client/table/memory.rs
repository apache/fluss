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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared, non-blocking budgets for retained read state.

use crate::error::{Error, Result};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Accounts for retained reader state across tasks in one query.
///
/// Implementations must be thread-safe, non-blocking and must not panic. A failed
/// reservation must leave usage unchanged and return [`Error::BufferExhausted`].
/// `release` returns previously reserved bytes and must not fail. Calls may run
/// on runtime threads without an engine's thread-local query context.
///
/// This is accounting, not an allocator: source I/O, temporary Arrow kernels and
/// batches handed to the consumer require their own resource management.
pub trait ReadMemoryBudget: Send + Sync + Debug {
    /// Atomically reserves bytes, or rejects the request without charging it.
    fn try_reserve(&self, bytes: usize) -> Result<()>;
    /// Returns bytes previously reserved by this reader.
    fn release(&self, bytes: usize);
}

/// A shared byte limit for retained reader state. It never waits or spills.
#[derive(Debug)]
pub struct LimitedReadMemoryBudget {
    limit: usize,
    used: AtomicUsize,
}

impl LimitedReadMemoryBudget {
    /// Creates an unused budget. A zero limit rejects all nonzero reservations.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    /// Current reserved bytes, including all readers sharing this budget.
    pub fn used_bytes(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

impl ReadMemoryBudget for LimitedReadMemoryBudget {
    fn try_reserve(&self, bytes: usize) -> Result<()> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let total = used
                .checked_add(bytes)
                .filter(|total| *total <= self.limit)
                .ok_or_else(|| Error::BufferExhausted {
                    message: format!(
                        "read memory budget exceeded: requested {bytes} bytes, \
                         {used} of {} bytes reserved",
                        self.limit,
                    ),
                })?;
            match self
                .used
                .compare_exchange_weak(used, total, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(current) => used = current,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub(super) struct Reservation {
    budget: Option<Arc<dyn ReadMemoryBudget>>,
    bytes: usize,
}

impl Reservation {
    pub(super) fn new(budget: Option<Arc<dyn ReadMemoryBudget>>) -> Self {
        Self { budget, bytes: 0 }
    }

    pub(super) fn budget(&self) -> Option<Arc<dyn ReadMemoryBudget>> {
        self.budget.clone()
    }

    pub(super) fn resize(&mut self, bytes: usize) -> Result<()> {
        if let Some(budget) = &self.budget {
            if bytes > self.bytes {
                budget.try_reserve(bytes - self.bytes)?;
            } else if bytes < self.bytes {
                budget.release(self.bytes - bytes);
            }
            self.bytes = bytes;
        }
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            if self.bytes != 0 {
                budget.release(self.bytes);
            }
        }
    }
}

/// Overflow is an exhausted budget, never an uncharged allocation.
pub(super) fn checked_bytes(bytes: Option<usize>) -> Result<usize> {
    bytes.ok_or_else(|| Error::BufferExhausted {
        message: "read memory accounting overflow".into(),
    })
}

/// Conservative charge for a std HashMap, including spare/control slots and
/// old/new tables during growth. It is an estimate, not allocator instrumentation.
pub(super) fn hash_table_bytes<K, V>(peak_entries: usize) -> Result<usize> {
    if peak_entries == 0 {
        return Ok(0);
    }
    checked_bytes(
        peak_entries
            .max(4)
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_mul(std::mem::size_of::<(K, V)>() + 1)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn concurrent_readers_cannot_each_claim_the_shared_limit() {
        let budget = Arc::new(LimitedReadMemoryBudget::new(100));
        let barrier = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let budget = budget.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        let mut reservation = Reservation::new(Some(budget));
                        barrier.wait();
                        let accepted = reservation.resize(100).is_ok();
                        barrier.wait();
                        accepted
                    })
                })
                .collect();
            assert_eq!(
                handles
                    .into_iter()
                    .map(|h| usize::from(h.join().unwrap()))
                    .sum::<usize>(),
                1
            );
        });
        assert_eq!(budget.used_bytes(), 0);
    }
}
