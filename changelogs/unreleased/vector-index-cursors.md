## feature/core

VECTOR supports public read views backed by frozen primary storage.
Bounded SELECT consumes an internal materialized-result cursor.
NEIGHBOR cursors keep batches of 32 results and rescan between batches.

Ordinary VECTOR pairs and keyed count are unsupported. Unkeyed count uses
visible primary-index rows. Bounded SELECT and search use HNSW.

VECTOR fields use embedding values and borrow inline float32 coordinates
without another dense copy. Queries accept an array or an embedding.
Rich search returns tuples, nil, distances with with_distance enabled.

Graph nodes are capped at twice the peak retained embedding count plus 64,
including when background compaction cannot run. Appending a node fails with
OutOfMemory at the cap; reusing a GC-safe slot remains allowed.
