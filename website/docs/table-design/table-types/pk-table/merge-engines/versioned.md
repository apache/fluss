---
sidebar_label: Versioned
sidebar_position: 3
---

# Versioned Merge Engine

The **Versioned Merge Engine** enables data updates based on version numbers or event timestamps. It ensures that only the row with the highest version number (or event timestamp) for a given primary key is retained. This mechanism is particularly useful for deduplicating or merging out-of-order data while guaranteeing eventual consistency with the upstream source.

By setting `'table.merge-engine' = 'versioned'`, users can update data based on a configured version column. Updates are performed when the latest value of the specified field is greater than or equal to the stored value. If the incoming value is less than the stored value or is null, no update will occur.

This feature is especially valuable as a replacement for [Deduplication](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/deduplication/) transformations in streaming computations. It simplifies workflows, reduces complexity, and improves overall efficiency.


:::note
When using the `versioned` merge engine, keep the following limitations in mind:
- **`UPDATE` and `DELETE` statements are not supported.**
- **Partial updates are not supported.**
- **`UPDATE_BEFORE` and `DELETE` changelog events are ignored automatically.**
:::

### Version Column

The version column is a column in the table that stores the version number (or event timestamp) of the data record.
When enabling the versioned merge engine, the version column must be explicitly specified using the property:

```sql
'table.merge-engine' = 'versioned',
'table.merge-engine.versioned.ver-column' = '<column_name>'
```

The version column can be of the following data types:
- `INT`
- `BIGINT`
- `TIMESTAMP`
- `TIMESTAMP(p)` (with precision)
- `TIMESTAMP_LTZ` (timestamp with local time zone)
- `TIMESTAMP_LTZ(p)` (timestamp with local time zone and precision)


## Example:

```sql title="Flink SQL"

CREATE TABLE VERSIONED (
    a INT NOT NULL PRIMARY KEY NOT ENFORCED,
    b STRING, 
    ts BIGINT
 ) WITH (
    'table.merge-engine' = 'versioned',
    'table.merge-engine.versioned.ver-column' = 'ts'
);
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v1', 1000);

-- insert data with ts < 1000, no update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v2', 999);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v1  | 1000 |
-- +---+-----+------+


-- insert data with ts > 1000, update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v3', 2000);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v3  | 2000 |
-- +---+-----+------+

-- insert data with ts = null, no update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v4', null);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v3  | 2000 |
-- +---+-----+------+

```
