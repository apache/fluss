---
title: "Command Line Interface"
sidebar_position: 2
---

# Fluss CLI

Fluss CLI is a powerful command-line tool that provides interactive SQL execution and administrative operations for Apache Fluss. It offers a unified, SQL-first interface for managing databases, tables, and data with built-in optimizations for high-performance retrieval.

## Overview

The Fluss CLI supports two main operation modes:
- **SQL Commands**: Execute DDL, DML, DQL, and metadata SQL statements.
- **Admin Commands**: Perform cluster inspection, management, and rebalancing operations.

### Key Features
- **Full SQL Support**: DDL (CREATE/DROP/ALTER), DML (INSERT/UPSERT/UPDATE/DELETE), and DQL (SELECT with WHERE filtering).
- **Interactive REPL**: A full-featured shell with command history and multi-line SQL support.
- **Query Optimization**: Automatic use of the **Lookup API** for primary key point queries and **LogScanner** for streaming data.
- **Client-Side Filtering**: WHERE clause support with comparison (`=`, `!=`, `<`, `>`, etc.) and logical operators (`AND`, `OR`).
- **Metadata Inspection**: Comprehensive `SHOW` commands for cluster, database, table, and snapshot metadata.
- **Complex Types**: Native support for `ARRAY`, `MAP`, and `ROW` types in both storage and query results.

## Installation

The Fluss CLI is included in the standard Fluss distribution. After [deploying Fluss](../install-deploy/overview.md), you can find the CLI script at:

```bash
<FLUSS_HOME>/bin/fluss-cli.sh
```

## Quick Start

### Interactive SQL Mode

Start an interactive SQL session to manage your cluster and data:

```bash
./bin/fluss-cli.sh sql -b localhost:9123
```

In the interactive shell, execute SQL statements ending with a semicolon (`;`):

```sql
fluss-sql> SHOW DATABASES;
Databases:
-----------
default
2 database(s)

fluss-sql> CREATE DATABASE mydb;
Database created successfully: mydb

fluss-sql> exit
```

### Execute SQL from Command Line or File

Execute a single statement:
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -e "SHOW DATABASES;"
```

Execute a SQL statement from a file:
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -f query.sql
```

:::note File Mode Limitation
The CLI currently supports executing **one SQL statement per file**. Multi-statement scripts (separated by semicolons) will result in a parse error. For batch operations, execute statements individually or use the interactive mode.
:::

## SQL Command Reference

### DDL Statements

#### Database Operations
```sql
CREATE DATABASE [IF NOT EXISTS] mydb;
DROP DATABASE mydb;
SHOW SCHEMAS; -- Alias for SHOW DATABASES
```

#### Table Operations
```sql
-- Create a Primary Key table
CREATE TABLE mydb.users (
    user_id BIGINT,
    name STRING,
    email STRING,
    created_at TIMESTAMP,
    PRIMARY KEY (user_id)
);

-- Manage partitions and schema
ALTER TABLE mydb.users ADD PARTITION (...);
ALTER TABLE mydb.users ADD COLUMN status STRING;
ALTER TABLE mydb.users SET ('table.property' = 'value');
DROP TABLE mydb.users;
```

### DML Statements

#### Insert and Upsert
```sql
-- Insert new rows (works for both PK and Log tables)
INSERT INTO mydb.users VALUES (1, 'Alice', 'alice@example.com', '2024-01-01 10:00:00');

-- Explicit Upsert (only for PK tables)
UPSERT INTO mydb.users VALUES (1, 'Alice Updated', 'alice@example.com', '2024-01-01 10:00:00');
```

:::note
For primary key tables, `INSERT` automatically uses upsert semantics. Use `UPSERT` for better code clarity.
:::

#### Update and Delete
```sql
-- Update specific fields (Requires WHERE with all PK columns)
UPDATE mydb.users SET name = 'Alice Smith' WHERE user_id = 1;

-- Delete rows (Requires WHERE with all PK columns)
DELETE FROM mydb.users WHERE user_id = 1;
```

:::warning Safety Restrictions
`UPDATE` and `DELETE` statements **must** include a `WHERE` clause specifying all primary key columns. Statements without a `WHERE` clause are prohibited to prevent accidental data loss.
:::

### DQL Statements (SELECT)

Query data with automatic optimization based on your `WHERE` clause:

```sql
-- Streaming Scan: Uses LogScanner to consume data
SELECT * FROM mydb.users;

-- Point Query: Automatically optimized with Lookup API
SELECT * FROM mydb.users WHERE user_id = 1;

-- Column Projection: Efficiently retrieve specific fields
SELECT user_id, name FROM mydb.users;

-- Client-Side Filtering: Filter results with WHERE conditions
SELECT * FROM mydb.users WHERE age > 30;
SELECT * FROM mydb.users WHERE name = 'Alice' AND age >= 25;
SELECT * FROM mydb.users WHERE status = 'active' OR role = 'admin';
```

#### Query Optimization Strategies

The CLI automatically selects the optimal execution strategy based on your query:

| Query Pattern | Optimization | Description |
|:---|:---|:---|
| `SELECT * FROM table` | **LogScanner** | Streaming scan for full table reads |
| `WHERE pk = value` | **Lookup API** | Point query for primary key equality (all PK columns required) |
| `WHERE non_pk > value` | **Client-Side Filtering** | Scan + filter results after retrieval |
| `WHERE pk = 1 AND age > 30` | **Hybrid** | Lookup API + client-side filtering on non-PK columns |

:::tip Point Query Optimization
When a `WHERE` clause specifies **all primary key columns** with equality conditions, the CLI uses the high-performance Lookup API. You will see the message: `Using point query optimization (Lookup API)`.
:::

#### WHERE Clause Support

The CLI supports client-side filtering with the following operators:

**Comparison Operators**:
- `=`, `!=`, `<>` (equality and inequality)
- `<`, `<=`, `>`, `>=` (relational comparisons)

**Logical Operators**:
- `AND` - All conditions must be true
- `OR` - At least one condition must be true

**Unsupported Operators**:
- `LIKE` - Pattern matching (e.g., `name LIKE '%Alice%'`)
- `IN` - Set membership (e.g., `id IN (1, 2, 3)`)
- `BETWEEN` - Range checks
- `IS NULL` / `IS NOT NULL` - Null checks

**Examples**:
```sql
-- Numeric comparisons
SELECT * FROM mydb.orders WHERE amount > 1000;
SELECT * FROM mydb.orders WHERE quantity <= 10;

-- String equality
SELECT * FROM mydb.users WHERE status = 'active';
SELECT * FROM mydb.users WHERE email != 'admin@example.com';

-- Complex logical conditions
SELECT * FROM mydb.events 
WHERE (event_type = 'login' OR event_type = 'logout') 
  AND timestamp > '2024-01-01 00:00:00';

-- Combine PK lookup with filtering
SELECT * FROM mydb.users 
WHERE user_id = 1001 AND status = 'active';
```

:::info Performance Notes
- **Client-side filtering** scans all rows from the server and filters locally in the CLI
- For large datasets, consider adding appropriate indexes or using Flink SQL for server-side processing
- Point queries (PK equality) are always faster than scans with client-side filtering
:::

#### Streaming vs Batch Mode (LIMIT Control)

For **Log tables** (tables without primary keys), the CLI provides streaming query support:

```sql
-- Streaming Mode: Continuously poll for new data
SELECT * FROM mydb.log_events;
-- Output: "Streaming mode: Continuously polling for new data (Ctrl+C to exit)"
--         "Idle timeout: 30 seconds"

-- Batch Mode: Read specific number of rows and exit
SELECT * FROM mydb.log_events LIMIT 100;
-- Output: Reads up to 100 rows, then exits immediately
```

**Query Behavior by Table Type**:

| Table Type | Query | Behavior |
|:---|:---|:---|
| Log table | `SELECT * FROM log_tbl` | ♾️ **Streaming**: Polls continuously until idle timeout (30s) |
| Log table | `SELECT * FROM log_tbl LIMIT 100` | ✅ **Batch**: Reads 100 rows then exits |
| PK table | `SELECT * FROM pk_tbl` | ✅ **Batch**: Full table scan and exit |
| PK table | `SELECT * FROM pk_tbl LIMIT 50` | ✅ **Batch**: Scans up to 50 rows then exits |

:::tip Use Cases
- **Streaming mode**: Monitor live data, tail log tables, real-time debugging
- **Batch mode**: Data inspection, sample queries, scripting with predictable exit behavior
:::

:::note Streaming Behavior Details
- **Polling interval**: 5 seconds between scan attempts
- **Idle timeout**: 30 seconds of no new data triggers automatic exit (configurable via `--streaming-timeout`)
- **Exit**: Press `Ctrl+C` to manually terminate streaming queries
:::

### Metadata Statements

| Command | Description |
|:---|:---|
| `SHOW DATABASES` | List all databases |
| `SHOW TABLES FROM <db>` | List tables in a database |
| `DESCRIBE <db>.<table>` | Show table schema and properties |
| `SHOW CREATE TABLE <db>.<table>` | Show the DDL statement for a table |
| `SHOW PARTITIONS FROM <db>.<table>` | List table partitions |
| `SHOW OFFSETS FROM <db>.<table>` | Show bucket offsets (EARLIEST/LATEST) |
| `SHOW KV SNAPSHOTS FROM <table>` | List Key-Value snapshots for PK tables |
| `SHOW LAKE SNAPSHOT FROM <table>` | Show Lake storage snapshot metadata |

### Admin & Management

```sql
-- Cluster Inspection
SHOW SERVERS;
SHOW CLUSTER CONFIGS;

-- Server Management
ALTER SERVER TAG ADD PERMANENT_OFFLINE TO (1,2);
ALTER SERVER TAG REMOVE PERMANENT_OFFLINE FROM (1,2);

-- Rebalance Operations
REBALANCE CLUSTER WITH GOALS (DISK_USAGE);
SHOW REBALANCE ID 123;
CANCEL REBALANCE ID 123;

-- Security (Requires Authorizer)
SHOW ACLS;
CREATE ACL ...;
```

## SQL Limitations

The Fluss CLI is optimized for administrative operations and lightweight queries. The following SQL features are **not supported**:

### Unsupported Query Features
- **Aggregations**: `COUNT(*)`, `SUM()`, `AVG()`, `MIN()`, `MAX()`, `GROUP BY`
- **Joins**: `INNER JOIN`, `LEFT JOIN`, cross-table queries
- **Subqueries**: Nested `SELECT` statements
- **Window Functions**: `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`
- **HAVING**: Post-aggregation filtering
- **ORDER BY**: Result sorting (results are in storage order)
- **OFFSET**: Result pagination offset (LIMIT is supported for batch mode control)
- **DISTINCT**: Duplicate elimination
- **LIKE/IN/BETWEEN**: Advanced string/range matching (basic `=`, `!=`, `<`, `>` are supported)
- **IS NULL/IS NOT NULL**: Null checks in WHERE clauses
- **CASE/WHEN**: Conditional expressions

:::tip Supported WHERE Operators
The CLI supports basic comparison operators (`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`) and logical operators (`AND`, `OR`) for client-side filtering. See [WHERE Clause Support](#where-clause-support) for details.
:::

### For Advanced Analytics
Use **Apache Flink** with the [Fluss connector](../engine-flink/getting-started.md) for:
- Complex aggregations and window operations
- Multi-table joins
- Stream processing and real-time analytics
- Stateful computations

:::tip When to Use CLI vs Flink
- **CLI**: Administrative tasks, data inspection, simple point queries, metadata management
- **Flink**: Production analytics, complex queries, stream processing, large-scale data transformations
:::

## Supported Data Types

| SQL Type | Fluss Type | Example |
|:---|:---|:---|
| `BOOLEAN`, `BOOL` | BOOLEAN | `col BOOLEAN` |
| `TINYINT` | TINYINT | `col TINYINT` |
| `SMALLINT` | SMALLINT | `col SMALLINT` |
| `INTEGER`, `INT` | INT | `col INT` |
| `BIGINT`, `LONG` | BIGINT | `col BIGINT` |
| `FLOAT` | FLOAT | `col FLOAT` |
| `DOUBLE` | DOUBLE | `col DOUBLE` |
| `DECIMAL(p,s)` | DECIMAL | `col DECIMAL(10,2)` |
| `VARCHAR`, `STRING`, `TEXT` | STRING | `col STRING` |
| `BINARY`, `BYTES` | BYTES | `col BYTES` |
| `DATE` | DATE | `col DATE` |
| `TIME(p)` | TIME | `col TIME(3)` |
| `TIMESTAMP(p)` | TIMESTAMP | `col TIMESTAMP(6)` |
| `TIMESTAMP_LTZ(p)` | TIMESTAMP_LTZ | `col TIMESTAMP_LTZ(6)` |
| `ARRAY<T>` | ARRAY | `col ARRAY<STRING>` |
| `MAP<K,V>` | MAP | `col MAP<STRING, INT>` |
| `ROW<...>` | ROW | `col ROW<name STRING, age INT>` |

### Complex Type Literals
The CLI supports literal syntax for complex types in `INSERT` and `UPSERT` statements:
- **Array**: `ARRAY['a', 'b', 'c']`
- **Map**: `MAP['k1', 1, 'k2', 2]`
- **Row**: `ROW('Bob', 30)`

## Configuration Options

### Connection Settings
The CLI requires at least one bootstrap server to discover the cluster.

| Option | Alias | Description |
|:---|:---|:---|
| `-b` | `--bootstrap-servers` | **Required**. List of coordinator/tablet servers (e.g., `host1:9123,host2:9123`) |
| `-c` | `--config` | Path to a `.properties` file containing Fluss client configurations |
| `-e` | `--execute` | Execute a single SQL statement and exit |
| `-f` | `--file` | Execute SQL statements from a file and exit |
| `-o` | `--output-format` | Output format: `table` (default), `csv`, `json`, `tsv` |

### Configuration File Example
Create a `client.properties` file for persistent settings:
```properties
bootstrap.servers=localhost:9123
client.connection.timeout.ms=30000
client.request.timeout.ms=60000
```
Use it with: `./bin/fluss-cli.sh sql -c client.properties`

### CLI Options

The CLI supports various options to customize its behavior:

| Option | Short | Description | Default |
|:---|:---|:---|:---|
| `--bootstrap-servers` | `-b` | Fluss bootstrap servers (host:port,host:port) | **Required** |
| `--execute` | `-e` | Execute SQL statement directly | |
| `--file` | `-f` | Execute SQL from file | |
| `--config` | `-c` | Configuration properties file | |
| `--output-format` | `-o` | Output format: `table`, `csv`, `json`, `tsv` | `table` |
| `--quiet` | `-q` | Suppress status messages (useful for piping) | `false` |
| `--streaming-timeout` | | Idle timeout in seconds for streaming queries | `30` |

#### Quiet Mode

Use quiet mode to suppress status messages, making output suitable for piping to other tools:

```bash
# Without quiet mode - includes status messages
./bin/fluss-cli.sh sql -b localhost:9123 -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# Executing SELECT on table: mydb.users
# +------------+------------+------------+
# ...

# With quiet mode - clean output only
./bin/fluss-cli.sh sql -b localhost:9123 -q -o csv -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# id,name,age
# 10,Alice,25
# 20,Bob,30
```

#### Configurable Streaming Timeout

For streaming queries (log tables without LIMIT), customize the idle timeout:

```bash
# Default 30-second timeout
./bin/fluss-cli.sh sql -b localhost:9123 -e "SELECT * FROM mydb.log_events"

# Custom 60-second timeout
./bin/fluss-cli.sh sql -b localhost:9123 --streaming-timeout 60 \
  -e "SELECT * FROM mydb.log_events"

# Combine with quiet mode for clean streaming
./bin/fluss-cli.sh sql -b localhost:9123 -q --streaming-timeout 10 -o json \
  -e "SELECT * FROM mydb.log_events"
```

### Output Formats

The CLI supports multiple output formats for query results, optimized for different use cases:

| Format | Description | Best For |
|:---|:---|:---|
| `table` | Human-readable ASCII table (default) | Interactive terminal use |
| `csv` | Comma-separated values | Data processing with tools like awk, Excel |
| `tsv` | Tab-separated values | Data processing, log analysis |
| `json` | JSON array of objects | Programmatic processing, APIs, jq |

#### Usage Examples

**Table Format (Default)**
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# +------------+------------+------------+
# | id         | name       | age        |
# +------------+------------+------------+
# | 10         | Alice      | 25         |
# | 20         | Bob        | 30         |
# +------------+------------+------------+
# 2 row(s)
```

**CSV Format**
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -o csv -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# id,name,age
# 10,Alice,25
# 20,Bob,30
```

**JSON Format**
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -o json -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# [
#   {"id": 10, "name": "Alice", "age": 25},
#   {"id": 20, "name": "Bob", "age": 30}
# ]
```

**TSV Format**
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -o tsv -e "SELECT * FROM mydb.users LIMIT 2"
# Output:
# id	name	age
# 10	Alice	25
# 20	Bob	30
```

#### Processing with Unix Tools

**CSV with awk**
```bash
# With quiet mode for clean piping
./bin/fluss-cli.sh sql -b localhost:9123 -q -o csv \
  -e "SELECT * FROM mydb.users LIMIT 10" | \
  awk -F',' 'NR>1 {print $2}'
# Extracts names column only
```

**JSON with jq**
```bash
# Filter JVM warnings before processing with jq
./bin/fluss-cli.sh sql -b localhost:9123 -q -o json \
  -e "SELECT * FROM mydb.users WHERE age > 25 LIMIT 10" 2>&1 | \
  grep -v "^WARNING:" | jq '.[].name'
# Extracts name field from JSON objects
```

**TSV for log analysis**
```bash
./bin/fluss-cli.sh sql -b localhost:9123 -q -o tsv \
  -e "SELECT * FROM mydb.events LIMIT 1000" | \
  cut -f2,3 | sort | uniq -c
# Count unique combinations of columns 2 and 3
```

:::tip Output Format Tips
- Use `table` for interactive terminal sessions and debugging
- Use `csv` or `tsv` when piping to `awk`, `sed`, or importing to Excel
- Use `json` when integrating with scripts, APIs, or processing with `jq`
- Use `-q/--quiet` flag to suppress status messages when piping output
- For JSON output with jq, filter JVM warnings: `2>&1 | grep -v "^WARNING:" | jq ...`
:::

## Interactive Shell Features

- **Multi-line Support**: Statements can span multiple lines. Use `->` indentation for clarity.
- **History**: Use Up/Down arrows to navigate previous commands.
- **Exit**: Type `exit`, `quit`, or press `Ctrl+D`.

## Testing and Development

The Fluss CLI module has comprehensive test coverage (>70%) ensuring reliability and correctness.

### Running Tests

To run all CLI tests:

```bash
cd fluss-cli
../mvnw test -Dcheckstyle.skip -Dspotless.check.skip
```

To run tests with coverage report:

```bash
cd fluss-cli
../mvnw clean test jacoco:report -Dtest.skip.coverage=false \
  -Dcheckstyle.skip -Dspotless.check.skip

# View coverage report
open target/site/jacoco/index.html
```

### Test Coverage by Package

| Package | Coverage | Description |
|:---|:---|:---|
| `org.apache.fluss.cli.format` | 97% | Output formatters (CSV, JSON, TSV, Table) |
| `org.apache.fluss.cli.util` | 79% | Utility classes (type converters, parsers) |
| `org.apache.fluss.cli.sql` | 72% | SQL execution and query optimization |
| `org.apache.fluss.cli.sql.executor` | 65% | Statement executors (DDL, DML, DQL) |

### Test Categories

**Formatter Tests** (`CsvFormatterTest`, `JsonFormatterTest`, `TsvFormatterTest`, `OutputFormatTest`)
- Header/footer formatting
- Escaping special characters (commas, quotes, newlines, tabs)
- NULL value handling
- Empty result sets

**SQL Executor Tests** (`SqlExecutorSelectTest`, `SqlExecutorDdlDmlShowTest`, etc.)
- Query optimization (lookup vs scan)
- Quiet mode behavior
- Streaming timeout configuration
- WHERE clause filtering
- LIMIT handling

**Utility Tests** (`DataTypeConverterTest`, `WhereClauseEvaluatorTest`, etc.)
- Complex type parsing (ARRAY, MAP, ROW)
- Expression evaluation
- Type conversion accuracy

### Example: Testing New Features

When adding new features to the CLI, follow these testing patterns:

```java
// Test quiet mode suppresses status messages
@Test
void testQuietModeSuppressesMessages() throws Exception {
    SqlExecutor executor = new SqlExecutor(
        connectionManager, 
        new PrintWriter(writer), 
        OutputFormat.CSV,  
        true  // quiet = true
    );
    executor.executeSql("SELECT * FROM db.tbl");
    String output = writer.toString();
    
    assertThat(output).doesNotContain("Executing SELECT");
    assertThat(output).contains("expected,data");
}

// Test custom timeout configuration
@Test
void testCustomStreamingTimeout() throws Exception {
    SqlExecutor executor = new SqlExecutor(
        connectionManager,
        new PrintWriter(writer),
        OutputFormat.TABLE,
        false,  // quiet = false
        60      // 60 second timeout
    );
    executor.executeSql("SELECT * FROM log_table");
    String output = writer.toString();
    
    assertThat(output).contains("Idle timeout: 60 seconds");
}
```

### Building from Source

```bash
# Build CLI module only
cd fluss-cli
../mvnw clean package -DskipTests

# Build with tests
../mvnw clean package

# The compiled JAR will be at:
# target/fluss-cli-<version>.jar
```

## Troubleshooting

### Connection Refused
- Ensure the Fluss cluster is running.
- Verify the bootstrap server address and port (default: `9123`).
- Test connectivity using `telnet <host> 9123`.

### NoClassDefFoundError: picocli/CommandLine
This occurs if the CLI JAR was built without its dependencies.
- Ensure you are using the JAR from the official distribution or the shaded JAR located at `fluss-cli/target/fluss-cli-*-SNAPSHOT.jar`.

### Table Not Found
- Always use the fully-qualified table path: `<database>.<table_name>`.
- Use `SHOW TABLES FROM <database>` to verify the table exists.

## What's Next?
- [Java Client](java-client.md): Learn to interact with Fluss programmatically.
- [Flink Integration](../engine-flink/getting-started.md): Use Fluss with Apache Flink for real-time analytics.
- [Table Design](../table-design/overview.md): Optimize your schema for Fluss.
