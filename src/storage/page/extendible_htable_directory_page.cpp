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
#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint32_t i = 0; i < MaxSize(); ++i) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // 利用最低有效位
  return hash & ((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  assert(bucket_idx < pow(2, max_depth_));
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx < pow(2, max_depth_));
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth_mask = GetLocalDepthMask(bucket_idx);
  auto local_depth = GetLocalDepth(bucket_idx);
  // case:(LD==0)
  return (bucket_idx & local_depth_mask) ^ (1 << (local_depth - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  assert(global_depth_ <= max_depth_);
  return global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ <= max_depth_);
  if (global_depth_ == max_depth_) {
    return;
  }
  // Growing the directory.
  uint32_t pre_size = Size();
  global_depth_++;
  uint32_t curr_size = Size();
  for (uint32_t i = pre_size; i < curr_size; ++i) {
    bucket_page_ids_[i] = bucket_page_ids_[i - pre_size];
    local_depths_[i] = local_depths_[i - pre_size];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ > 0) {
    global_depth_--;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }
  for (uint32_t i = 0; i < Size(); ++i) {
    if (GetLocalDepth(i) == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  double size_float = pow(2, global_depth_);
  auto size = static_cast<uint32_t>(size_float);
  return size;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  double tmp = pow(2, max_depth_);
  auto max_size = static_cast<uint32_t>(tmp);
  return max_size;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(bucket_idx >= 0);
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx] += 1; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  assert(bucket_idx >= 0);
  local_depths_[bucket_idx] -= 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  auto local_depth = GetLocalDepth(bucket_idx);
  return (1 << local_depth) - 1;
}

}  // namespace bustub
