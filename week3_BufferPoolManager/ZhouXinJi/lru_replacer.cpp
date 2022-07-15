//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> locker(mutex_);
  if (list_.empty()) {
    return false;
  }
  *frame_id = list_.back();
  table_.erase(*frame_id);
  list_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(mutex_);
  if (table_.find(frame_id) != table_.end()) {  // frame_id exists
    auto iter = table_[frame_id];
    table_.erase(frame_id);
    list_.erase(iter);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(mutex_);
  if (table_.find(frame_id) == table_.end()) {  // frame_id doesn't exist
    list_.push_front(frame_id);
    table_[frame_id] = list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> locker(mutex_);
  return list_.size();
}

}  // namespace bustub
