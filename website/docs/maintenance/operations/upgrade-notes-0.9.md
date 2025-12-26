---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.8 to v0.9

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.8 and Fluss 0.9. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.9.

## Adding Columns

Fluss storage format was designed with schema evolution protocols from day one. Therefore, tables created in v0.8 or earlier can immediately benefit from the `ADD COLUMN` feature after upgrading to v0.9 without dropping and re-creating table.
However, old clients (pre-v0.9) do not recognize the new schema versioning protocol. If an old client attempts to read data from a table that has undergone schema evolution, it may encounter decoding errors or data inaccessibility.
Therefore, it is crucial to ensure that all Fluss servers and all clients interacting with the Fluss cluster are upgraded to v0.9 before performing any schema modifications.

:::warning
Attempting to add columns while older clients (v0.8 or below) are still actively reading from the table will lead to Schema Inconsistency and may crash your streaming pipelines.
:::

## Deprecation / End of Support

TODO