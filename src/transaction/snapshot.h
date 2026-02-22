#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

struct Snapshot {
  uint64_t snapshot_id = 0;
  uint64_t parent_snapshot_id = 0;
  int64_t table_id = 0;
  std::string operation; // "append", "overwrite", "delete"
  int32_t added_files_count = 0;
  int32_t deleted_files_count = 0;
  std::string committed_at; // ISO 8601 timestamp

  bool is_ancestor_of(uint64_t other_parent) const {
    return snapshot_id <= other_parent;
  }
};

inline std::vector<Snapshot>
build_snapshot_chain(const std::vector<Snapshot> &all_snapshots,
                     uint64_t from_snapshot_id) {
  std::vector<Snapshot> chain;

  std::unordered_map<uint64_t, const Snapshot *> index;
  for (const auto &s : all_snapshots) {
    index[s.snapshot_id] = &s;
  }

  uint64_t current = from_snapshot_id;
  while (current != 0) {
    auto it = index.find(current);
    if (it == index.end())
      break;
    chain.push_back(*it->second);
    current = it->second->parent_snapshot_id;
  }

  return chain;
}
