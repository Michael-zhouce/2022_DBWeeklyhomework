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
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

//lru管理的是frame
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(lru_latch_);
  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.front();//最先进去的，符合lru
  lru_map_.erase(*frame_id); //hash表删除
  lru_list_.pop_front();  //list删除
  return true;
}

//理解成有人正在使用这个页面，不能用来作为牺牲了，故需要删除当前frame_id
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(lru_latch_);
  auto it = lru_map_.find(frame_id);
  if (it == lru_map_.end()) {
    return;
  }

  lru_list_.erase(it->second); //删除frame_id对应的值
  lru_map_.erase(it);  //hash表删除
}

//lru_relpacer不需要考虑什么时候需要unpin，直接理解成当前frame_id可以作为牺牲id，装入队列就行
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(lru_latch_);
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;
  }
  lru_list_.push_back(frame_id); //添加到list末尾
  auto p = lru_list_.end();
  lru_map_[frame_id] = --p;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(lru_latch_);
  return lru_list_.size();
}

}  // namespace bustub
