<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# DDL metadata protocol checks

These bounded TLA+ models check ordinary database/table DDL admission and
identity-safe metadata publication. They use independent contracts and explicit
negative controls. `run.py` generates configurations and saves commands, source
and configuration hashes, Java/TLC versions, complete logs and counterexamples
outside the source tree.

## Contracts and implementation mapping

| Model | Contract and source mapping |
| --- | --- |
| `DdlAdmission.tla` | Refines `DdlGateContract.tla`. `DdlAdmission.acquire` checks and installs a permit atomically. Each database state has an exclusive flag and a set of active table names. Database DDL excludes every child DDL, same-table DDL excludes its peers, and different-table DDL can overlap. |
| `DdlTree.tla` | Refines `DdlTreeContract.tla`. Registration is withdrawn before schema/partition cleanup. Cached parent metadata remains while known partition metadata exists. Recovery and watcher callbacks are separate transitions. |
| `DdlProtocol.tla` | Refines `DdlMetadataContract.tla`. A captured leader context acquires admission before `ZooKeeperMetadataContext.beginOperation`. Each DROP and CREATE is a separate call, with its own permit, barrier and metadata reads. Every `ZooKeeperMetadataOperation.multi` carries the original epoch and uses the captured ZooKeeper object. |

The admission model represents Java database-state object identity with local
instance counters. Permits retain that instance and leader context. Closing a
permit removes its table or exclusive flag and removes an empty database entry.
Duplicate close follows the closed-token no-op branch. Negative controls target
database/table conflict checks and empty-entry cleanup.

The publication contract protects an existing object's physical identity.
Table/partition IDs are separate from node versions. Table registration versions
start and recur at 1, and partition versions start and recur at 0. New CREATE
operations establish fresh identities. A partition CREATE also retains its
parent table identity. Cached task input binds only the parent table ID, matching
the automatic-partition task entry point. Partition identities are read afresh
inside the protected operation.

Successful ZooKeeper reads wait for earlier writes in their own session.
`ReadsOrdered` represents that FIFO response boundary. A request records its
session when submitted, so later client replacement cannot reinterpret an
already submitted request. Unknown client completion releases admission while
the server can still process the submitted request. The model gives that request
no additional application submissions after its call has returned.

The external-body marker checks an epoch that was already superseded at operation
entry. It represents the initial validation before a call such as Lake descriptor
lookup. Leadership can change after a successful check. Schema alteration
synchronizes changes to Lake before registering the Fluss schema in ZooKeeper.
A successful Lake call can be followed by a failed ZooKeeper write, including an
epoch rejection. Lake effects and metadata publication are separate operations.

## Reproduce

Use Python 3 and Java 11. Download the pinned TLA+ Tools release jar. The release
tag is v1.7.4, its embedded TLC version is 2.19, and its revision is
`5a47802b5c391f59ecdd44117981f4ff8c0656ba`.
[Official release](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)

```sh
curl -fL https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar \
  -o /tmp/tla2tools.jar
python3 tools/verification/ddl-metadata/run.py \
  --jar /tmp/tla2tools.jar --out /tmp/fluss-ddl-checks
```

The runner requires SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`
and one TLC worker. The output directory must be new. Select individual cases
with repeated `--case` arguments. `--java` accepts a Java executable and
`--timeout` sets each case's time limit in seconds. A timeout, unexpected error,
missing witness or wrong invariant causes a nonzero runner exit.

## Checked matrix

Results are recorded after executing the configurations in `run.py` with the
pinned jar and Java 11. The matrix includes independent contract refinement,
metadata incarnation safety, admission accounting and conditional progress.

| Check | Completed finite domain |
| --- | --- |
| Admission safety/refinement | Same table, different table, different database. Respectively 278, 312 and 204 distinct states. |
| Registration safety/refinement | Table and partition targets, each 3,341 distinct states. |
| Leader/session changes | Failover 71,218 states and session replacement 8,393 states. |
| Input and creation paths | Cached parent table ID 2,027 states, current-name request 2,399 states, competing CREATE calls 535 states. |
| Superseded epoch at body entry | 9 states. |
| Interrupted DROP and callback retirement | Withdrawal/recovery 879 states, two-table cascade 12,501 states, successful retirement and missing-assignment retirement each 165 states, unknown completion without failover 243 states. |
| Weakening controls | Fourteen expected counterexamples cover missing admission conflicts/cleanup, shared DDL, missing epoch fence, session rebinding, stale cached parent ID and missing initial epoch validation. They also detect schema-first interruption, premature parent retirement, count-index errors and unguarded orphan callbacks. |
| Reachability | Six expected witnesses cover different-table concurrency, entry reallocation, two recreations, unknown completion followed by server application, retained namespace after unknown withdrawal and recovery with an empty registration. |

All 36 configurations completed with the pinned jar and OpenJDK 11.0.30+7-LTS.
Sixteen safety/refinement configurations passed. Fourteen weakening controls and
six reachability witnesses produced their named invariant counterexamples.

A counterexample is the expected result for a weakening control or a reachability
witness. The rebind control permits an earlier session's submitted request to
finish while a replacement session changes metadata in the same epoch. That
schedule is an explicit transport-failure assumption. The original-epoch check
rejects obsolete requests once a new epoch is published.

## Visibility, recovery and retirement

A published table registration always has an available schema. A DROP first
writes empty registration data with the original epoch and observed root
version. It then removes descendants through individually fenced operations.
Database cascade applies the same withdrawal to each table before removing its
subtree. The empty registration uses the same invisible representation as a
staged CREATE.

The watcher converts full-to-empty registration into the existing table-drop
event and ignores physical deletion of an already empty registration. Parent
retirement waits for every known partition metadata ID, including partitions
without assignments. The per-table count index equals that metadata set's
cardinality. Zero counts remove their map entries. Its size is bounded by the
number of known partition metadata IDs.

After failover, initialization skips empty registrations and queues orphan
assignment IDs through the existing cleanup path. Initial-sync partition events
with unknown parents are ignored. Late drop events retain their physical-ID
cleanup path while skipping unavailable table-dependent metrics and stores.

An unknown or failed DROP can leave an invisible namespace subtree. The model
contains an explicit witness of that state. Residual schema and partition znodes
are reclaimed by `dropTable(ignoreIfNotExists=true)`, CREATE or database cascade.
Failover recovery retires orphan assignments and replicas.

## Verification scope

- Admission uses three calls, two databases, two table names and two leader
  contexts. Its empty-entry and entry-count invariants describe the
  `DdlAdmission` database-state map.
- Registration publication uses two representative partitions of one table. The
  ordinary mutation and two independent DROP/CREATE pairs run once each. Leader
  epoch and session replacement each change at most once. A smaller creator-race
  case uses one DROP and two CREATE calls.
- Interrupted cleanup uses one or two tables, two partition metadata IDs per
  table, one captured operation and one epoch change. It distinguishes schema
  availability, physical child nodes, cached partition metadata and assignment
  IDs. The count index is checked through retirement and recovery reload.
- Conditional progress assumes fair request processing, delivery of relevant
  watcher callbacks and completion of admitted cleanup work. Successful DROP
  cases check eventual cache retirement. Unknown-completion cases check safety
  and failover recovery without requiring unconditional namespace cleanup.
- Valid registration is the visibility boundary for the multi-step CREATE path.
  The three model families have separate finite domains. Arbitrary subtree
  shapes, mixed concurrent creation, Java memory visibility, object reachability
  and executor scheduling are outside these state spaces.

The concrete Java coverage is organized by implementation boundary.

| Test class | Boundary |
| --- | --- |
| `DdlAdmissionTest` | Scope exclusion, permit closure and database-state cleanup. |
| `ZooKeeperClientTest` | Original-epoch checks on recursive deletion, withdrawal interrupted by fencing, empty-root cleanup, replaced-session rejection and session changes during reads. |
| `MetadataManagerTest` | Rejection of a superseded leader before entering a DDL body and staged-registration recovery. |
| `CoordinatorContextTest` | Partition-count insertion/removal/reset and conditional reverse-path binding. |
| `CoordinatorEventProcessorTest` | Withdrawn-table startup, unknown-parent events and late physical-ID deletion. |
| `AutoPartitionManagerTest` | Automatic-partition admission conflicts and retry scheduling. |

Automatic-partition retry coalescing and the pending-retry map belong to the
Java scheduler coverage. They are separate from both formal map bounds.
Intermediate partial-CREATE states belong to the concrete ZooKeeper path rather
than the registration-publication projection.
