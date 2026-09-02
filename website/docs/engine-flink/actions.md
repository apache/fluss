---
sidebar_label: Actions
title: Actions
sidebar_position: 11
---

# Flink Actions

Fluss ships maintenance jobs, called actions, inside the Flink connector jar. Run an action with `flink run`:

```shell
<FLINK_HOME>/bin/flink run fluss-flink-1.20-$FLUSS_VERSION$.jar <action> [options]
```

Pass `--help` to list the available actions, or `<action> --help` to print the options of one action.
Actions are available in the connectors for Flink 1.19 and later; the Flink 1.18 connector does not include them.
Actions access remote storage directly, so the [filesystem jar](/downloads#filesystem-jars) for your remote storage must be in `<FLINK_HOME>/lib`.

## remove_orphan_files

Deletes files in remote storage that no table references, such as leftovers from failed uploads or interrupted table drops.
The action runs as a Flink batch job: it fetches the active log manifests and KV snapshots from the coordinator, scans the `log/` and `kv/` directories of the remote storage root (lakehouse data is not touched), and deletes files that are unreferenced and older than the cutoff.

```shell
<FLINK_HOME>/bin/flink run fluss-flink-1.20-$FLUSS_VERSION$.jar remove_orphan_files \
    --bootstrap-server localhost:9123 \
    --all-databases \
    --dry-run
```

| Option                                 | Default       | Description                                                                                                                                                                                                                                                                        |
|----------------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--bootstrap-server`                   | (required)    | Fluss bootstrap servers, e.g. `localhost:9123`.                                                                                                                                                                                                                                    |
| `--database`                           | (required)    | Database to clean. Exactly one of `--database` or `--all-databases` is required; the two are mutually exclusive.                                                                                                                                                                    |
| `--all-databases`                      | (required)    | Clean every database. Exactly one of `--database` or `--all-databases` is required; the two are mutually exclusive.                                                                                                                                                                 |
| `--table`                              | (none)        | Restrict the cleanup to one table in `--database`. Also disables the orphan-table scan for that database.                                                                                                                                                                          |
| `--older-than`                         | now - 3 days  | Cutoff as an ISO-8601 timestamp with an offset, e.g. `2024-01-01T00:00:00Z`. Only files modified before it are deleted. Must be at least 1 day ago so that in-flight uploads are never deleted, which makes the action safe to run on a live cluster.                              |
| `--dry-run`                            | false         | Report what would be deleted without deleting anything.                                                                                                                                                                                                                            |
| `--parallelism`                        | Flink default | Parallelism of the scan-and-delete stage.                                                                                                                                                                                                                                          |
| `--remote-fs-op-rate-limit-per-second` | 100           | Best-effort job-wide target for remote filesystem operations (listing, manifest reads and deletes). The scan-and-delete stage splits it across `--parallelism` subtasks with a minimum of 1 op/s each.                                                                             |
| `--allow-delete-manifest`              | false         | Also delete orphan `.manifest` files. They are kept by default because deleting an active one breaks the bucket's metadata chain.                                                                                                                                                  |
| `--allow-clean-orphan-tables`          | false         | Delete the contents of table directories the coordinator no longer knows about. By default they are only reported as `action=skip_orphan_table` in the audit log.                                                                                                                  |
| `--allow-clean-orphan-partitions`      | false         | Delete the contents of partition directories the coordinator no longer knows about. By default they are only reported as `action=skip_orphan_partition` in the audit log.                                                                                                          |
| `--conf <key>=<value>`                 | (none)        | Extra configuration, repeatable. `fs.*` keys configure the remote filesystem with the same keys as `server.yaml` (e.g. `fs.oss.accessKeyId`); `client.*` keys (e.g. `client.security.protocol`) are passed to the Fluss client, see [authentication](/security/authentication.md). |

Notes:

- Run with `--dry-run` first and review the `fluss.orphan.audit` logger in the Flink TaskManager logs. Each line carries an `action=` such as `would_delete`, `would_delete_dir`, `deleted`, `dir_deleted`, `skip_unknown`, `skip_orphan_table`, `skip_orphan_partition` or `bucket_aborted`, and the run ends with an `action=summary` line. To list the files inside orphan table or partition directories, combine `--dry-run` with the corresponding `--allow-clean-orphan-*` flag.
- Shared SST files under the `shared/` directory of a primary key table are never deleted, even inside orphan table or partition directories, because the set of shared files still in use is tracked by the TabletServers and is not visible to the action.
- The cutoff is fixed when the job starts, so files written while a long scan is running are never considered.
