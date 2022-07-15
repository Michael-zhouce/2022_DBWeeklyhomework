//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  start_index = 0;  // 构造时start_index初始为0
  managers_ = new BufferPoolManagerInstance *[num_instances];
  for (size_t i = 0; i < num_instances; i++) {
    managers_[i] = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {  // 释放指针所指对象空间
    delete managers_[i];
  }
  delete[] managers_;  // 释放指针空间
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return managers_[page_id % num_instances_];  // mod映射
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  /*  round robin，从start_index开始转一圈，转回到start_index,看看哪个BufferPoolManager能new成功，
    都不行的话返回空指针*/
  for (size_t i = 0; i < num_instances_; i++) {  // 分配page
    std::lock_guard<std::mutex> guard(latch_);   // 加锁
    BufferPoolManager *manager = *(managers_ + start_index);
    Page *page = manager->NewPage(page_id);
    start_index = (start_index + 1) % num_instances_;  // start_index+1取模
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *manager = GetBufferPoolManager(page_id);
  return manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    managers_[i]->FlushAllPages();
  }
}

}  // namespace bustub
