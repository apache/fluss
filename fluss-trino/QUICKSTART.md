# Fluss Trino Connector - Quick Start Guide

## Overview

The Fluss Trino Connector enables Trino to query Apache Fluss tables, providing OLAP capabilities over real-time streaming data.

## Installation

### 1. Build the Connector

```bash
cd fluss-trino
mvn clean package -DskipTests
```

### 2. Deploy to Trino

```bash
# Create plugin directory
mkdir -p $TRINO_HOME/plugin/fluss

# Copy the connector JAR (choose the appropriate version)
cp fluss-trino-435/target/fluss-trino-435-*.jar $TRINO_HOME/plugin/fluss/

# Or for version 436:
# cp fluss-trino-436/target/fluss-trino-436-*.jar $TRINO_HOME/plugin/fluss/
```

### 3. Configure Catalog

Create `$TRINO_HOME/etc/catalog/fluss.properties`:

```properties
connector.name=fluss
bootstrap.servers=localhost:9092
```

### 4. Restart Trino

```bash
$TRINO_HOME/bin/launcher restart
```

## Usage Examples

### Connect to Trino CLI

```bash
trino --server localhost:8080 --catalog fluss
```

### Basic Queries

```sql
-- List all databases
SHOW SCHEMAS FROM fluss;

-- List tables in a database
SHOW TABLES FROM fluss.my_database;

-- Describe table structure
DESCRIBE fluss.my_database.my_table;

-- Select data
SELECT * FROM fluss.my_database.my_table LIMIT 10;

-- Filter with WHERE clause
SELECT id, name, age 
FROM fluss.my_database.users 
WHERE age > 18 AND city = 'Beijing';

-- Aggregate query
SELECT country, COUNT(*) as user_count, AVG(age) as avg_age
FROM fluss.my_database.users
GROUP BY country
ORDER BY user_count DESC;

-- JOIN query
SELECT 
    u.id,
    u.name,
    o.order_id,
    o.total_amount
FROM fluss.db1.users u
INNER JOIN fluss.db1.orders o ON u.id = o.user_id
WHERE o.order_date >= DATE '2024-01-01';
```

### Advanced Features

#### Column Pruning
Only specified columns are read from storage:
```sql
SELECT id, name FROM fluss.db.users;  -- Only id and name are read
```

#### Predicate Pushdown
Filter conditions are pushed to storage layer:
```sql
SELECT * FROM fluss.db.orders WHERE order_date >= DATE '2024-01-01';
```

#### Limit Pushdown
Limit is applied at storage layer:
```sql
SELECT * FROM fluss.db.users LIMIT 100;
```

## Configuration Reference

### Required Configuration

| Property | Description | Example |
|----------|-------------|---------|
| `connector.name` | Connector identifier | `fluss` |
| `bootstrap.servers` | Fluss server addresses | `localhost:9092` |

### Optional Configuration

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `connection.max-idle-time` | Max connection idle time | `10m` | `15m` |
| `request.timeout` | Request timeout | `60s` | `30s` |
| `scanner.fetch.max-wait-time` | Scanner max wait time | `500ms` | `1s` |
| `scanner.fetch.min-bytes` | Scanner min fetch bytes | `1MB` | `512KB` |
| `union-read.enabled` | Enable union read | `true` | `false` |
| `column-pruning.enabled` | Enable column pruning | `true` | `false` |
| `predicate-pushdown.enabled` | Enable predicate pushdown | `true` | `false` |
| `limit-pushdown.enabled` | Enable limit pushdown | `true` | `false` |
| `max-splits-per-second` | Max splits per second | `1000` | `500` |
| `max-splits-per-request` | Max splits per request | `100` | `50` |

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to Fluss cluster

**Solution**: 
- Verify `bootstrap.servers` configuration
- Check network connectivity to Fluss servers
- Review Trino server logs in `$TRINO_HOME/var/log/server.log`

### Performance Issues

**Problem**: Queries are slow

**Solution**:
- Enable query optimizations (column pruning, predicate pushdown)
- Increase `max-splits-per-second` for better parallelism
- Check Fluss cluster health and resources

### Data Type Issues

**Problem**: Unsupported data type error

**Solution**:
- Check supported type mappings in README.md
- Verify table schema in Fluss

## Supported Features

- ✅ Read from LogStore
- ✅ Read from KvStore  
- ✅ Column pruning
- ✅ Predicate pushdown
- ✅ Limit pushdown
- ✅ All basic data types
- ✅ Complex types (ARRAY, MAP, ROW)
- ⏳ Union Read (real-time + historical)
- ⏳ Aggregate pushdown
- ⏳ Write support

## Support

- Documentation: https://fluss.apache.org/docs
- Issues: https://github.com/apache/fluss/issues
- Mailing List: dev@fluss.apache.org
