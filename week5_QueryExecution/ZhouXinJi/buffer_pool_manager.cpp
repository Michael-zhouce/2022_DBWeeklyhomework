//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (page_table_.find(page_id) != page_table_.end()) {  // page_id exists
    frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_ += 1;
    return page;
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (!replacer_->Victim(&frame_id)) {
    return nullptr;
  } else {
    Page *page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  }
  page_table_[page_id] = frame_id;
  Page *page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, page->GetData());
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  // assert(page_table_.find(page_id) != page_table_.end());
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  page->is_dirty_ = page->is_dirty_ || is_dirty;
  if (page->pin_count_ <= 0) {  // unpin falied
    return false;
  }
  page->pin_count_ -= 1;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  // Make sure you call DiskManager::WritePage!
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (!replacer_->Victim(&frame_id)) {
    return nullptr;
  } else {  // Only the replaced page should be written to disk.
    Page *page = &pages_[frame_id];
    page_table_.erase(page->GetPageId());
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
  }
  Page *page = &pages_[frame_id];
  *page_id = disk_manager_->AllocatePage();
  page_table_[*page_id] = frame_id;
  page->ResetMemory();
  page->pin_count_ = 1;
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }
  page_table_.erase(page->GetPageId());
  page->page_id_ = INVALID_PAGE_ID;
  disk_manager_->DeallocatePage(page_id);
  free_list_.push_front(frame_id);
  replacer_->Pin(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  for (auto &iter : page_table_) {
    Page *page = &pages_[iter.first];
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
  }
}

}  // namespace bustub
