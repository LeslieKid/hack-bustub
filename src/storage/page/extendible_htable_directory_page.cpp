//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) { max_depth_ = max_depth; }

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto help = 1 << (global_depth_ - 1);
  return help ^ bucket_idx;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return (1 << global_depth_) - 1; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  uint32_t tmp = pow(2, global_depth_) - 1;
  global_depth_++;
  for (uint32_t i = 1; i <= tmp + 1; ++i) {
    bucket_page_ids_[tmp + i] = bucket_page_ids_[i - 1];
    local_depths_[tmp + i] = local_depths_[i - 1];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  bool can_shrink = true;
  for (auto &depth : local_depths_) {
    if (depth == global_depth_) {
      can_shrink = false;
      break;
    }
  }
  return can_shrink;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  auto tmp = pow(2, global_depth_);
  auto size = static_cast<uint32_t>(tmp);
  return size;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return (1 << local_depths_[bucket_idx]) - 1;
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
