#pragma once

#include <cstdint>
#include <string>
#include <vector>

/**
 * Immutable representation of a committed snapshot.
 *
 * A snapshot captures the state of a table at a specific point in time:
 *   - Which partitions were added or removed relative to its parent.
 *   - The operation type (append, overwrite, delete).
 *
 * Snapshots form a singly-linked chain via parent_snapshot_id,
 * enabling time-travel queries: "give me the table as of snapshot 42".
 */
struct Snapshot {
    uint64_t    snapshot_id        = 0;
    uint64_t    parent_snapshot_id = 0;
    int64_t     table_id           = 0;
    std::string operation;               // "append", "overwrite", "delete"
    int32_t     added_files_count  = 0;
    int32_t     deleted_files_count = 0;
    std::string committed_at;            // ISO 8601 timestamp

    /**
     * Check if this snapshot is an ancestor of another by walking
     * the snapshot chain. Useful for conflict resolution.
     */
    bool is_ancestor_of(uint64_t other_parent) const {
        return snapshot_id <= other_parent;
    }
};

/**
 * Utility to walk the snapshot chain and collect the history.
 *
 * Given a list of snapshots (ordered by committed_at DESC),
 * returns the chain from the given snapshot back to the root.
 */
inline std::vector<Snapshot> build_snapshot_chain(
    const std::vector<Snapshot>& all_snapshots,
    uint64_t from_snapshot_id)
{
    std::vector<Snapshot> chain;

    // Index snapshots by ID for O(1) lookup
    std::unordered_map<uint64_t, const Snapshot*> index;
    for (const auto& s : all_snapshots) {
        index[s.snapshot_id] = &s;
    }

    uint64_t current = from_snapshot_id;
    while (current != 0) {
        auto it = index.find(current);
        if (it == index.end()) break;
        chain.push_back(*it->second);
        current = it->second->parent_snapshot_id;
    }

    return chain;
}
