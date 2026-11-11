## feature/core

VECTOR supports exact unbounded ALL/NEIGHBOR cursors and public read views
backed by frozen primary storage. Bounded SELECT consumes an index cursor.
NEIGHBOR cursors keep batches of 32 results and rescan between batches.

Packed varbinary vector fields borrow little-endian float32 coordinates
without a separate dense copy. box.vector.pack/unpack convert the format.
Rich search returns tuples, nil, distances with with_distance enabled.

Graph nodes are capped at twice the peak retained embedding count plus 64,
including when background compaction cannot run. New embeddings fail with
OutOfMemory at the cap until maintenance releases space.
