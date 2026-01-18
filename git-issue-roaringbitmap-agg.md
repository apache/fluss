Title: Add RoaringBitmap aggregation support to Fluss merge engine

Description:
Fluss aggregation merge engine needs native RoaringBitmap aggregation functions for 32-bit and
64-bit integer sets. The functions should merge serialized bitmap bytes stored in BYTES columns
and be available through both SQL/connector properties and the Java Schema API. Implementation
should align with Paimon behavior and provide documentation and tests.

Solution:
- Introduce new aggregation function types (rbm32/rbm64) and Java factory helpers.
- Implement FieldRoaringBitmap32Agg/FieldRoaringBitmap64Agg and register factories via SPI.
- Add RoaringBitmapUtils for reusable serialize/deserialize helpers with bounded buffer reuse.
- Update aggregation documentation with rbm32/rbm64 usage examples.
- Add unit tests for aggregation behavior and AggregationContext mapping.
- Add RoaringBitmap dependency and shade relocation in fluss-server.
