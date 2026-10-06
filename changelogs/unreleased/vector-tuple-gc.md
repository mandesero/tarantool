## bugfix/core

Ordinary memtx releases VECTOR bindings to obsolete tuples after commit
and to discarded tuples after rollback. Retired HNSW nodes and dense
vectors remain allocated until rebuild; rebuild can now omit these nodes.
