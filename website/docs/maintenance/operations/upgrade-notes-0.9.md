---
title: Upgrade Notes
sidebar_position: 4
---

# Upgrade Notes from v0.8 to v0.9

These upgrade notes discuss important aspects, such as configuration, behavior, or dependencies, that changed between Fluss 0.8 and Fluss 0.9. Please read these notes carefully if you are planning to upgrade your Fluss version to 0.9.

## Add Column At Last.
If you're only upgrading to Fluss v0.9 without modifying table schemas, you can safely skip this section.

However, If you plan to add columns to existing tables, follow these mandatory steps:
1. Upgrade all Fluss servers to v0.9 
Ensure complete server infrastructure is running v0.9 before attempting any schema modifications.
2. Restart and upgrade Fluss client to v0.9
Old clients are incompatible with mixed schema versions and cannot properly read data with different schemas. This includes flink job, tier service and java client.

**Skipping these steps when adding columns may result in schema inconsistency issues or data inaccessibility.**

## Deprecation / End of Support

TODO