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

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

typedef std::unordered_map<page_id_t, frame_id_t> page_map;

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

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!

  latch_.lock();

  page_map iter = page_table_.find(page_id);   //查询页面id到缓冲池帧id的映射
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
      latch_.unlock();
      return false;
  }

  frame_id_t frame_id = iter->second;
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);    //刷出到磁盘

  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!

  latch_.lock();

  for(size_t i = 0; i < pool_size_; i++) {                  //将缓冲池中的全部页面刷出到磁盘
      disk_manager_->WritePage(pages_[i], pages_[i].data_);
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {     //申请新页面
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();

  frame_id_t frame_id;

  if(!free_list_.empty()) {                  //缓冲池有空闲页面，进行分配
      frame_id = free_list_.back();
      free_list_.pop_back();
  }else {                                    //缓冲池满，进行页面替换
      if(!replacer_->Victim(&frame_id)) {
          return nullptr;
      }
  }

  *page_id = AllocatePage();                 //分配磁盘页面

  Page *page = &pages_[frame_id];
  if(page->IsDirty()){                       //读入内存后页面被修改过，则应刷到磁盘
          disk_manager_->WritePage(page->page_id_, page->data_);
  }

  page_table_.erase(page->page_id_);        //更新页表
  page_table_[*page_id] = frame_id;

  page->page_id_ = *page_id;                //更新页面元数据
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  replacer_->Pin(frame_id);                 //固定页面并返回页面指针
  disk_manager_->WritePage(*page_id, page->data_);      //新页面写入磁盘

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {    //根据page_id获取页面指针
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();

  page_map iter = page_table_.find(page_id);  //(通过页表)查询磁盘中页面到缓冲池中的帧的映射
  frame_id_t frame_id;

  if(iter != page_table_.end()) {       //find()返回正向迭代器，当查找失败时返回的迭代器与end()方法相同
      frame_id = iter->second;
      pages_[frame_id].pin_count_++;    //该页面pin计数加1
      replacer_->Pin(frame_id);         //调用Pin()固定页面

      latch_.unlock();

      Page *page = &pages_[frame_id];   //返回页面指针
      return page;
  }else {
      if(!free_list_.empty()) {        //缓冲池中有空闲页面，进行分配
          frame_id = free_list_.back();
          free_list_.pop_back();
      }else {                          //缓冲池满，调用Victim()进行页面替换
          if(!replacer_.Victim(&frame_id)) {
              return nullptr;         //无可替换页面
          }
      }
      Page *page = &pages_[frame_id];
      if(page->IsDirty()) {            //读入内存后页面被修改过，则应刷到磁盘
          disk_manager_->WritePage(page->page_id_, page->data_);
      }
      page_table_.erase(page->page_id_);    //更新页表
      page_table_[page_id] = frame_id;

      page->page_id = page_id;              //更新页面元数据
      page->pin_count_ = 1;
      page->is_dirty_ = false;

      replacer_->Pin(frame_id);             //固定页面并返回页面指针
      disk_manager_->ReadPage(page_id, page->data_);

      return page;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();

  page_map iter = page_table_.find(page_id);

  if(iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
      latch_.unlock();
      return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];      //找到页面指针

  if(page->pin_count_ > 0) {           //被引用的页面不能被删除
      latch_.unlock();
      return false;
  }

  if(page->IsDirty()) {                //在内存中已被修改的页面应刷到磁盘后删除
      disk_manager_->WritePage(page->page_id_, page->data_);
  }

  page_table_.erase(page_id);
  DeallocatePage(page_id);             //磁盘取消分配页面

  page->page_id_ = INVALID_PAGE_ID;    //更新页面元数据
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  free_list_.push_back(frame_id);     //将页面加入空闲队列

  latch_.unlock();
  return false;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {

  latch_.lock();

  page_map iter = page_table_.find(page_id);

  if(iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
      latch_.unlock();
      return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  page->is_dirty_ = is_dirty;

  if(page->pin_count_ > 0) {      //若页面被引用计数大于0则减1
      page->pin_count_--;
  }else {                         //否则表示页面未被引用，直接返回
      latch_.unlock();
      return false;
  }

  if(page->pin_count_ == 0) {     //若引用计数减1后清零则调用Unpin()取消对页面的固定
      replacer_->Unpin(frame_id);
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
