# Data types

How each Fluss type is declared in a `Fluss.Schema` and represented as an Elixir value. Values
are the same for writing and reading.

| Fluss type | Schema | Elixir value |
|---|---|---|
| BOOLEAN | `:boolean` | `true` / `false` |
| TINYINT, SMALLINT, INT, BIGINT | `:tinyint`, `:smallint`, `:int`, `:bigint` | integer, range-checked on write |
| FLOAT, DOUBLE | `:float`, `:double` | float |
| STRING | `:string` | `String.t()` |
| CHAR(n) | `{:char, n}` | `String.t()` |
| DECIMAL(p, s) | `{:decimal, p, s}` | string, e.g. `"12.34"`; rounded half-up to `s` on write |
| BYTES | `:bytes` | binary |
| BINARY(n) | `{:binary, n}` | binary |
| DATE | `:date` | integer: days since 1970-01-01 |
| TIME | `:time` | integer: milliseconds since midnight |
| TIMESTAMP | `:timestamp` | `{epoch_millis, nanos_of_milli}`, no time zone |
| TIMESTAMP_LTZ | `:timestamp_ltz` | `{epoch_millis, nanos_of_milli}`, a UTC instant |
| ARRAY | `{:array, type}` | list |
| MAP | `{:map, key_type, value_type}` | map |
| ROW | `{:row, [{"field", type}]}` | map keyed by field-name atoms |

Converting to standard library types:

```elixir
date = Date.add(~D[1970-01-01], days)
time = Time.add(~T[00:00:00], millis_of_day, :millisecond)

{millis, nanos} = timestamp
utc = DateTime.add(DateTime.from_unix!(millis, :millisecond), nanos, :nanosecond)  # :timestamp_ltz
naive = DateTime.to_naive(utc)                                                   # :timestamp
```

## Nullability

`nil` is null; `{:not_null, type}` forbids it (see `Fluss.Schema`). Primary key columns and map
keys are always `NOT NULL`: `Fluss.Admin.get_table_schema/4` returns them wrapped even if
declared bare.

## Not supported yet

- TIME and TIMESTAMP precision can't be declared; `:time` is TIME(0) and `:timestamp` /
  `:timestamp_ltz` are precision 6.
- A FLOAT or DOUBLE holding NaN or ±Infinity (writable from other clients) can't be read:
  Erlang has no such values.
