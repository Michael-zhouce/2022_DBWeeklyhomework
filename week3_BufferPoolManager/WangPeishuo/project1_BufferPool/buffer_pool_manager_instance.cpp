//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer_pool_manager_instance.h"

#include "macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool
{
    latch_.lock();
    if (page_id == INVALID_PAGE_ID)
    {
        latch_.unlock();
        return false;
    }
    frame_id_t frame_id;
    if (page_table_.count(page_id)) //如果有这个page_id
    {
        frame_id = page_table_[page_id];
        Page& page = pages_[frame_id];
        disk_manager_->WritePage(page_id, page.data_);
        page.is_dirty_ = false; //已经不是脏页了
        latch_.unlock();
        return true;
    }
    else
    {
        latch_.unlock();
        return false;
    }
}

void BufferPoolManagerInstance::FlushAllPgsImp()
{
    latch_.lock();
    frame_id_t id;
    for (id = 1; id <= pool_size_; id++)
    {
        FlushPgImp(pages_[id].page_id_);
    }
    latch_.unlock();
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page *
{
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    latch_.lock();
    frame_id_t frame_id;
    if (!free_list_.empty()) //如果free_list不空，从这里找出一个地方来存
    {
        frame_id = free_list_.front(); //从freelist的头部拿出一个frame_id来
        free_list_.pop_front();
    }
    else   //BufferPool满了，所以需要LRU一下才行
    {
        if (!replacer_->Victim(&frame_id))
        {
            std::cout << "NULL!" << std::endl;
            latch_.unlock();
            return nullptr;
        }
    }
    //frame_id就是腾出来的地方
    if (pages_[frame_id].is_dirty_) //脏了
    {//pages_[frame_id]为旧页
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //写
    }
    *page_id = AllocatePage();

    page_table_.erase(pages_[frame_id].page_id_); //先更新哈希表

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    page_table_[pages_[frame_id].page_id_] = frame_id;

    disk_manager_->WritePage(*page_id, pages_[frame_id].data_); //从磁盘中读入数据
    replacer_->Pin(frame_id);
    latch_.unlock();
    return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
    latch_.lock();
    
    if (page_table_.count(page_id)) //如果有这个page_id
    {
        frame_id_t frame_id = page_table_[page_id];
        //准备使用这个frame，需要PIN
        replacer_->Pin(frame_id);
        pages_[frame_id].pin_count_++; //这个页的pin数自增
        latch_.unlock();
        return &pages_[frame_id]; //将这个page的地址返回
    }
    else   //如果没有这个page_id
    {
        frame_id_t frame_id;
        if (!free_list_.empty()) //如果free_list不空，从这里找出一个地方来存
        {
            frame_id = free_list_.front(); //从freelist的头部拿出一个frame_id来
            free_list_.pop_front(); 
        }
        else   //BufferPool满了，所以需要LRU一下才行
        {
            if (!replacer_->Victim(&frame_id))
            {
                std::cout << "NULL!" << std::endl;
                latch_.unlock();
                return nullptr;
            }
        }
        //现在已经找到了frame_id，下面需要用新的page霸占这个地方，但是需要先看原页是否脏
        if (pages_[frame_id].is_dirty_) //脏了
        {//pages_[frame_id]为旧页
            disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //写
        }
        //下面正式进行替换
        page_table_.erase(pages_[frame_id].page_id_); //先更新哈希表

        pages_[frame_id].page_id_ = page_id;
        pages_[frame_id].pin_count_ = 1;
        pages_[frame_id].is_dirty_ = false;
        page_table_[pages_[frame_id].page_id_] = frame_id;

        disk_manager_->ReadPage(page_id, pages_[frame_id].data_); //从磁盘中读入数据
        replacer_->Pin(frame_id);
        latch_.unlock();
        return &pages_[frame_id];
    }
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    latch_.lock();
    if (!page_table_.count(page_id))
    {
        latch_.unlock();
        return true;
    }
    frame_id_t frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ > 0)
    {
        latch_.unlock();
        return false;
    }
    //可以删除这个frame的情况
    if (pages_[frame_id].is_dirty_) //脏了
    {//pages_[frame_id]为旧页
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //写
    }

    page_table_.erase(page_id);
    DeallocatePage(page_id);

    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;

    free_list_.push_back(frame_id);
    latch_.unlock();
    return false;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool
{
    latch_.lock();

    if (!page_table_.count(page_id))
    {
        latch_.unlock();
        return false;
    }

    frame_id_t frame_id;
    frame_id = page_table_[page_id];
    //if(is_dirty) disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //写
    pages_[frame_id].is_dirty_ = is_dirty; //只改变一下是否脏就可以了，不用这个时候去写！
    int count = --pages_[frame_id].pin_count_;
    if (count <= 0)
    {
        replacer_->Unpin(frame_id);
        latch_.unlock();
        return false;
    }
    else
    {
        latch_.unlock();
        return true;
    }
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t
{
    const page_id_t next_page_id = next_page_id_;
    next_page_id_ += num_instances_;
    ValidatePageId(next_page_id);
    return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const
{
    assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
