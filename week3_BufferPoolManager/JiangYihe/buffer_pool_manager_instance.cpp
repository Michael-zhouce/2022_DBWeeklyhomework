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

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);  // 加锁
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;                  // hash表维护page_id和frame_id的映射
  Page *page = pages_ + frame_id;                      // 数组寻址（基地址+偏移量）
  page->is_dirty_ = false;                             // 把is_dirty_置为false（已经flush了）
  disk_manager_->WritePage(page_id, page->GetData());  // 写入磁盘
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.begin();
  while (iter != page_table_.end()) {
    page_id_t page_id = iter->first;
    frame_id_t frame_id = iter->second;                  // hash表维护page_id和frame_id的映射
    Page *page = pages_ + frame_id;                      // 数组寻址（基地址+偏移量）
    disk_manager_->WritePage(page_id, page->GetData());  // 写入磁盘
    iter++;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {  // 存在空余页
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {  // 根据lru算法淘汰一页
    page = &pages_[frame_id];

    if (page->IsDirty()) {  // 脏页flush
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());  // 从page table中移除
  }

  if (page != nullptr) {
    // 创建新页
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory();
    replacer_->Pin(frame_id);
    return page;
  }
  return nullptr;  // buffer pool中所有页面都被固定，返回nullptr
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {  // 原来就在buffer里，直接访问

    frame_id_t frame_id = iter->second;  // hash表维护page_id和frame_id的映射
    Page *page = &pages_[frame_id];      // 拿到page
    page->pin_count_++;                  // pin_count_++
    replacer_->Pin(frame_id);            // 通知replacer
    return page;
  }

  // 从free list中获取
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {  // 存在空余页
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {  // 根据lru算法淘汰一页
    page = &pages_[frame_id];

    if (page->IsDirty()) {                                           // 脏页flush
      disk_manager_->WritePage(page->GetPageId(), page->GetData());  // 写入磁盘
    }
    page_table_.erase(page->GetPageId());  // 从page_table_中移除
  }

  if (page != nullptr) {
    // 新页
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;

    disk_manager_->ReadPage(page_id, page->GetData());  // 写入磁盘
    page_table_[page_id] = frame_id;
    replacer_->Pin(frame_id);  // replacer把它pin住
  }
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {  // 页面不存在则返回true（见上）
    return true;
  }

  frame_id_t frame_id = iter->second;  // hash表维护page_id和frame_id的映射
  Page *page = pages_ + frame_id;      // 数组寻址（基地址+偏移量）
  if (page->pin_count_ > 0) {          // pin_count_ > 0时不能返回
    return false;
  }

  if (page->IsDirty()) {  // 如果这个page被修改过则要写回磁盘
    disk_manager_->WritePage(page_id, page->GetData());
  }
  replacer_->Pin(frame_id);    // 从replacer中删除该页
  page_table_.erase(page_id);  // 从hash表中移除
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  return true;
}

// 完成了对这个页的操作，需要unpin
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {  // 如果page_table_中没有，返回false
    return false;
  }

  frame_id_t frame_id = iter->second;  // hash表维护page_id和frame_id的映射
  Page *page = pages_ + frame_id;      // 数组寻址（基地址+偏移量）
  if (page->GetPinCount() <= 0) {      //<=0直接return
    return false;
  }

  if (is_dirty) {                // 如果已经dirty了
    page->is_dirty_ = is_dirty;  // 不能直接赋值
  }
  page->pin_count_--;  // 这是正常情况，pin_count_--

  /*  如果这个页的pin_count==0我们需要给它加到Lru_replacer中。
     因为没有人引用它。所以它可以成为被替换的候选人*/
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub