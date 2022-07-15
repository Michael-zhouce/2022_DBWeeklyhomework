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


//task1 
#include "buffer/ .h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }

void LRUReplacer::Pin(frame_id_t frame_id) {}

void LRUReplacer::Unpin(frame_id_t frame_id) {}

auto LRUReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
//LRUReplacer初始化时是空的，只有新的unpin的frame会在LRUReplacer
bool LRUReplacer::Victim(frame_id_t *frame_id) {//块从缓冲区移走 并被写回磁盘 
  std::lock_guard latch(lru_lock);

  if (lru_map.empty()) return false;//如果为空，则返回false 

  frame_id_t frame = lru_cache.back(); // 返回最后一个应该移走的 ,未删除 
  lru_cache.pop_back();// 删除 
  lru_map.erase(frame);在hushmap里也删除 
  *frame_id = frame;

  return true;
}
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard latch(lru_lock);//上锁，为了线程安全 

  if (lru_map.count(frame_id) != 0) {//如果还在使用则这个块不能删除 
    lru_cache.erase(lru_map[frame_id]); // 从cache中删除, 这样就不会被移走了
    lru_map.erase(frame_id);
  }
}
void LRUReplacer::Unpin(frame_id_t frame_id) { // 添加到cache中, frame可以被删除了，没有线程在使用这个块了 

    std::lock_guard latch(lru_lock);
    if (lru_map.count(frame_id) != 0) return;


  while (this->Size() >= capacity) { 

    frame_id_t del_frame = lru_cache.front();
    lru_cache.pop_front();
    lru_map.erase(del_frame);
  }

  lru_cache.push_front(frame_id);
  lru_map[frame_id] = lru_cache.begin();
}

size_t LRUReplacer::Size() {
  std::lock_guard latch(lru_lock);
  return lru_cache.size();//返回缓冲区的大小 
}




