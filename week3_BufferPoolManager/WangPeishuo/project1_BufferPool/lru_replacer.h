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
	 std::list<frame_id_t> IdList; //һ��˫����������ÿ��frame��id�����иո�ʹ�ù���id���ö���ÿ��LRU���ͷŵ����һ��
	 int capacity;                 //Buffer Pool�������������IdList�����Ԫ�ظ���
	 
	 //��id������˫��������ָ�����id�ĵ�����
	 std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> Hash; 

	 std::mutex mtx; //������
};

}  // namespace bustub
