---
slug: /
sidebar_position: 1
title: Introduction
---

# Introduction

[Apache Fluss](https://fluss.apache.org/) (incubating) is a streaming storage system built for real-time analytics, serving as the real-time data layer for Lakehouse architectures.

This documentation covers the **Fluss client libraries** for Rust, Python, C++, and [Java](https://fluss.apache.org/docs/0.9/apis/java-client/), which are developed in the [fluss-rust](https://github.com/apache/fluss-rust) repository (Java client is part of the [main Fluss project](https://github.com/apache/fluss)). These clients allow you to:

- **Create and manage** databases, tables, and partitions
- **Write** data to log tables (append-only) and primary key tables (upsert/delete)
- **Read** data via log scanning and key lookups
- **Integrate** with the broader Fluss ecosystem including lakehouse snapshots

## Prerequisites

You need a running Fluss cluster to use any of the client libraries. See the [Deploying a Local Cluster](https://fluss.apache.org/docs/install-deploy/deploying-local-cluster/) guide to get started.

## Key Concepts

- **Log table** — an append-only table (no primary key). Records are immutable once written. Use for event streams, logs, and audit trails.
  - **Offset** — the position of a record within a log table's bucket. Used to track reading progress. Start from `EARLIEST_OFFSET` to read all data, or resolve the current latest offset via `list_offsets` to only read new records.
- **Primary key (PK) table** — a table with a primary key. Supports upsert, delete, and point lookups.
- **Bucket** — the unit of parallelism within a table (similar to Kafka partitions). Each table has one or more buckets. Readers subscribe to individual buckets.
- **Partition** — a way to organize data by column values (e.g. by date or region). Each partition contains its own set of buckets. Partitions must be created explicitly before writing.

## Client Overview

|                        | Rust                                                       | Python                   | C++                                            |
|------------------------|------------------------------------------------------------|--------------------------|------------------------------------------------|
| **Package**            | [fluss-rs](https://crates.io/crates/fluss-rs) on crates.io | Build from source (PyO3) | Build from source (CMake)                      |
| **Async runtime**      | Tokio                                                      | asyncio                  | Synchronous (Tokio runtime managed internally) |
| **Data format**        | Arrow RecordBatch / GenericRow                             | PyArrow / Pandas / dict  | Arrow RecordBatch / GenericRow                 |
| **Log tables**         | Read + Write                                               | Read + Write             | Read + Write                                   |
| **Primary key tables** | Upsert + Delete + Lookup                                   | Upsert + Delete + Lookup | Upsert + Delete + Lookup                       |
| **Partitioned tables** | Read + Write                                               | Read + Write             | Read + Write                                   |

## How This Guide Is Organised

The **Clients** section walks through installation, configuration, and working with each table type across all three languages. Code examples are shown side by side under **Rust**, **Python**, and **C++** headings.

The **Contributing** guide covers building from source, running tests, and the release process for contributors.
