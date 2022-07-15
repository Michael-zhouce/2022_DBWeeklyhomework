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

LRUReplacer::LRUReplacer(size_t num_pages) {
    capacity = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *victim) -> bool {
    latch.lock();

    if (LRUMap.empty()) {
        latch.unlock();
        return false;
        //无可替换帧，直接返回false
    }

    victim = lru_list.back();        //选择LRU列表尾部，即为最近最久未使用的帧
    frame_id_t victime_id = *victim;
    LRUMap.erase(victim_id);         //将该帧从LRU列表和哈希表中删除
    LRUList.pop_back();

    latch.unlock();
    return true;                    //返回true
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch.lock();

    //用pin操作固定一个页面后，该页面不可再被置换
    if (lRUMap.count(frame_id) != 0) {
        //若该页面在LRU队列中，应该将该帧从LRU列表和哈希表中删除
        LRUList.erase(LRUMap[frame_id]);
        LRUMap.erase(frame_id);
    }

    latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch.lock();

    if(LRUMap.count(frame_id) != 0){
        latch.unlock();
        return;
    }

    //若用Unpin取消对一个页面的固定，且该页面不在LRU队列中，应将其加入LRU列表和哈希表
    else {
        LRUList.push_front(frame_id);       //将该帧加入队列首部
        LRUMap[frame_id] = LRUList.begin();
    }

    latch.unlock();
}

auto LRUReplacer::Size() -> size_t {

    return LRUList.size();

}

}  // namespace bustub
