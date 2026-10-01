## Feature

- Add a configurable VECTOR storage procedure for vshard protected calls.
  It limits results to the requested live buckets, returns a versioned
  envelope through IProto CALL, and enforces a relative search budget.
