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
//LRUReplacer��ʼ��ʱ�ǿյģ�ֻ���µ�unpin��frame����LRUReplacer
bool LRUReplacer::Victim(frame_id_t *frame_id) {//��ӻ��������� ����д�ش��� 
  std::lock_guard latch(lru_lock);

  if (lru_map.empty()) return false;//���Ϊ�գ��򷵻�false 

  frame_id_t frame = lru_cache.back(); // �������һ��Ӧ�����ߵ� ,δɾ�� 
  lru_cache.pop_back();// ɾ�� 
  lru_map.erase(frame);��hushmap��Ҳɾ�� 
  *frame_id = frame;

  return true;
}
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard latch(lru_lock);//������Ϊ���̰߳�ȫ 

  if (lru_map.count(frame_id) != 0) {//�������ʹ��������鲻��ɾ�� 
    lru_cache.erase(lru_map[frame_id]); // ��cache��ɾ��, �����Ͳ��ᱻ������
    lru_map.erase(frame_id);
  }
}
void LRUReplacer::Unpin(frame_id_t frame_id) { // ��ӵ�cache��, frame���Ա�ɾ���ˣ�û���߳���ʹ��������� 

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
  return lru_cache.size();//���ػ������Ĵ�С 
}




