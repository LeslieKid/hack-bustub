#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// Move Constructor
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard read_guard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard write_guard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  auto &guard = this->guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
    guard.page_->RUnlatch();
  }
  guard.page_ = nullptr;
  guard.bpm_ = nullptr;
  guard.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  auto &guard = this->guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
    guard.page_->WUnlatch();
  }
  guard.page_ = nullptr;
  guard.bpm_ = nullptr;
  guard.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
