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
	this->capacity = num_pages; //初始化Buffer Pool最大的容量
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
	mtx.lock();
	if (IdList.size() == 0) //如果没有元素
	{
		frame_id = nullptr; //返回空
		mtx.unlock();
		return false;       //false
	}
	else
	{
		frame_id = &IdList.back();
		IdList.pop_back();         //删除最后一个元素
		Hash.erase(*frame_id);     //哈希表中也删除
		mtx.unlock();
		return true;
	}
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
	mtx.lock();

	//先看看Hash里面有没有这个元素，有就暂时删掉
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
		Hash[frame_id] = IdList.begin(); //Unpin的frame看作刚用完，所以要头插
	}

	mtx.unlock();
}

auto LRUReplacer::Size() -> size_t 
{
	return IdList.size();
}

}  // namespace bustub
