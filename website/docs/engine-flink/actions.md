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
Use a connector for Flink 1.19 or later that matches your Flink runtime, together with a Fluss cluster that supports the action's metadata APIs.
Actions access remote storage directly, so the [filesystem jar](/downloads#filesystem-jars) for your remote storage must be in `<FLINK_HOME>/lib`.

## remove_orphan_files

Removes eligible orphan files from Fluss remote storage, such as leftovers from failed uploads or interrupted table drops.
The action runs as a Flink batch job: it fetches active log manifests and KV snapshots from the coordinator, scans the corresponding `log/` and `kv/` directories, and deletes recognized files that are unreferenced and older than the cutoff. Lakehouse data is outside its scope.

For existing tables, cleanup covers remote log files in buckets with committed manifests and private snapshot files in buckets with active KV snapshots. Shared SST files are retained. Within databases known to the coordinator, cleanup can also include orphan table and partition directories when enabled with the options below.

```shell
<FLINK_HOME>/bin/flink run fluss-flink-1.20-$FLUSS_VERSION$.jar remove_orphan_files \
    --bootstrap-server localhost:9123 \
    --all-databases \
    --dry-run
```

| Option                                 | Default       | Description                                                                                                                                                                                                                                                                                                                                                                                        |
| -------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--bootstrap-server`                   | (required)    | Fluss bootstrap servers, e.g. `localhost:9123`.                                                                                                                                                                                                                                                                                                                                                    |
| `--database`                           | (required)    | Database to clean. Exactly one of `--database` or `--all-databases` is required; the two are mutually exclusive.                                                                                                                                                                                                                                                                                   |
| `--all-databases`                      | (required)    | Clean every database. Exactly one of `--database` or `--all-databases` is required; the two are mutually exclusive.                                                                                                                                                                                                                                                                                |
| `--table`                              | (none)        | Restrict the cleanup to one table in `--database`. Also disables the orphan-table scan for that database.                                                                                                                                                                                                                                                                                          |
| `--older-than`                         | now - 3 days  | Cutoff as an ISO-8601 timestamp with an offset, e.g. `2024-01-01T00:00:00Z`. Only files modified before it are deleted. Must be at least 1 day ago. Choose a retention interval longer than the expected duration of uploads and recovery operations, and ensure the filesystem provides accurate modification times.                                                                              |
| `--dry-run`                            | false         | Report what would be deleted without deleting anything.                                                                                                                                                                                                                                                                                                                                            |
| `--parallelism`                        | Flink default | Parallelism of the scan-and-delete stage.                                                                                                                                                                                                                                                                                                                                                          |
| `--remote-fs-op-rate-limit-per-second` | 100           | Best-effort job-wide target for remote filesystem operations (listing, manifest reads and deletes). The scan-and-delete stage splits it across `--parallelism` subtasks with a minimum of 1 op/s each; when parallelism exceeds the configured target, the aggregate scan rate can exceed that target.                                                                                             |
| `--allow-delete-manifest`              | false         | Also delete orphan `.manifest` files. They are kept by default because deleting an active one breaks the bucket's metadata chain.                                                                                                                                                                                                                                                                  |
| `--allow-clean-orphan-tables`          | false         | Clean eligible files in table directories the coordinator no longer knows about. By default they are only reported as `action=skip_orphan_table` in the audit log.                                                                                                                                                                                                                                 |
| `--allow-clean-orphan-partitions`      | false         | Clean eligible files in partition directories the coordinator no longer knows about. By default they are only reported as `action=skip_orphan_partition` in the audit log.                                                                                                                                                                                                                         |
| `--conf <key>=<value>`                 | (none)        | Extra configuration, repeatable. `fs.*` keys configure remote filesystem access; `client.*` keys configure the Fluss client, see [authentication](/security/authentication.md). Filesystem settings may also be needed in the client namespace during scope enumeration. For OSS, supply matching `fs.oss.*` and `client.fs.oss.*` settings (for example, `fs.oss.region` and `client.fs.oss.region`). The accepted keys depend on the filesystem plugin. |

Notes:

- In dry-run mode, use the `would_delete` events to inspect candidate files. Summary deletion and reclaimed-byte counters represent planned work in this mode, not changes to storage. Directory counts include only directories that are already empty when inspected.
- Run with `--dry-run` first and review the `fluss.orphan.audit` logger in the Flink TaskManager logs. Each line carries an `action=` such as `would_delete`, `would_delete_dir`, `deleted`, `dir_deleted`, `skip_unknown`, `skip_orphan_table`, `skip_orphan_partition` or `bucket_aborted`, and an `action=summary` line reports aggregate counters. Review skip and failure events alongside the summary to understand which paths were processed; job completion alone does not mean every path was cleaned. To list the files inside orphan table or partition directories, combine `--dry-run` with the corresponding `--allow-clean-orphan-*` flag.
- Shared SST files under the `shared/` directory of a primary key table are retained, including inside orphan table or partition directories. The action does not resolve shared SST references in this version.
- The cutoff is fixed when the job starts. Eligibility is determined from filesystem modification times relative to that cutoff.
