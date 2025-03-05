---
sidebar_label: FirstRow
sidebar_position: 2
---

# FirstRow Merge Engine

By setting `'table.merge-engine' = 'first_row'` in the table properties, users can retain the first record for each primary key.
This configuration generates an insert-only changelog, allowing downstream Flink jobs to treat the PrimaryKey Table as an append-only Log Table.
As a result, downstream transformations that do not support retractions/changelogs, such as [Window Aggregations](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/window-agg/)
and [Interval Joins](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/joins/#interval-joins), can be applied seamlessly.

This feature is particularly valuable for replacing log deduplication in streaming computations, reducing complexity and improving overall efficiency.

:::note
When using `first_row` merge engine, there are the following limits:

- `UPDATE` and `DELETE` SQL statements are not supported
- Partial Update is not supported
- `UPDATE_BEFORE` and `DELETE` changelog events are ignored automatically
:::

## Example

```sql title="Flink SQL"
CREATE TABLE T (
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
    'table.merge-engine' = 'first_row'
);

INSERT INTO T VALUES (1, 2.0, 't1');
INSERT INTO T VALUES (1, 3.0, 't2');

SELECT * FROM T;

-- Output
-- +---+-----+------+
-- | k | v1  | v2   |
-- +---+-----+------+
-- | 1 | 2.0 | t1   |
-- +---+-----+------+
```