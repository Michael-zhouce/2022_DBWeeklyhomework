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

#include "lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages)
{
	this->capacity = num_pages; //��ʼ��Buffer Pool��������
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
	mtx.lock();
	if (IdList.size() == 0) //���û��Ԫ��
	{
		frame_id = nullptr; //���ؿ�
		mtx.unlock();
		return false;       //false
	}
	else
	{
		frame_id = &IdList.back();
		IdList.pop_back();         //ɾ�����һ��Ԫ��
		Hash.erase(*frame_id);     //��ϣ����Ҳɾ��
		mtx.unlock();
		return true;
	}
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
	mtx.lock();

	//�ȿ���Hash������û�����Ԫ�أ��о���ʱɾ��
	if (Hash.count(frame_id))
	{
		IdList.erase(Hash[frame_id]);
		Hash.erase(frame_id);
	}

	mtx.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
	mtx.lock();

	if (Hash.count(frame_id))
	{
		IdList.push_front(frame_id);
		Hash[frame_id] = IdList.begin(); //Unpin��frame���������꣬����Ҫͷ��
	}

	mtx.unlock();
}

auto LRUReplacer::Size() -> size_t 
{
	return IdList.size();
}

}  // namespace bustub
