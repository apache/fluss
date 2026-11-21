<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility fixtures

Generated with Apache DataSketches Java 6.2.0 and Memory 3.0.2 on JDK 17.
Update an HllSketch(12) and KllDoublesSketch.newHeapInstance() once with each integer from 1
through 20000 (long for HLL, double for KLL), then serialize with toCompactByteArray() and
toByteArray(), respectively. These exercise HLL estimation mode and compacted KLL levels.

KLL compaction is randomized, so regenerating produces equivalent distributions rather than
identical bytes. These fixtures contain synthetic numeric inputs only.
