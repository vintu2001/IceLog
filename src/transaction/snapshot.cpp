#include "snapshot.h"

// The Snapshot struct and build_snapshot_chain are fully defined in the header.
// This translation unit exists for future non-inline helpers and to satisfy
// the CMake source list.
//
// Potential future additions:
//   - Snapshot serialization/deserialization for on-disk manifest files
//   - Snapshot diff computation (delta between two snapshots)
//   - Snapshot expiration policies (retain last N, or retain last 7 days)
