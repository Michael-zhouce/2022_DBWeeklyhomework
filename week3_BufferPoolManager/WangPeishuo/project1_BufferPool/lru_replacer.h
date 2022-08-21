//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>

#include "replacer.h"
#include "config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
	 std::list<frame_id_t> IdList; //一个双向链表，保存每个frame的id，其中刚刚使用过的id会置顶，每次LRU会释放掉最后一个
	 int capacity;                 //Buffer Pool的最大容量，即IdList的最大元素个数
	 
	 //给id，返回双向链表中指向这个id的迭代器
	 std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> Hash; 

	 std::mutex mtx; //互斥量
};

}  // namespace bustub
