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
                                                     LogManager *log_manager)
    : num_instances_(num_instances_),             //构造多缓冲池并分配内存
      pool_size_(pool_size_),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // Allocate and create individual BufferPoolManagerInstances

  buffers_ = new BufferPoolManagerInstance* [num_instances];   //动态分配缓冲池类型指针数组

  for(size_t i = 0; i < num_instances_; i++) {                 //为每个缓冲池分配内存
      buffers_[i] = new BufferPoolManagerInstance(pool_size_, num_instances_, i, disk_manager_, log_manager_);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {      //释放内存
  for(size_t i = 0; i < num_instances_; i++) {
      delete buffers_[i];
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  buffer_id_t buffer_id = page_id % num_instances_;          //根据页号获取缓冲池编号并返回缓冲池指针
  return buffers_[buffer_id];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance buffer = GetBufferPoolManager(page_id);
  return buffer->FetchPgImp(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance buffer = GetBufferPoolManager(page_id);
  return buffer->UnpinPgImp(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance buffer = GetBufferPoolManager(page_id);
  return buffer->FlushPgImp(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  Page *page = nullptr;
  for(size_t i = 0; i < num_instances_; i++) {        //遍历多缓冲池
      page = buffers_[instance_index_]->NewPgImp(page_id);
      if(page != nullptr) {
          return page;
      }
      instance_index_ = (instance_index_ + 1) % num_instances_;   //移动当前缓冲池索引
  }
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance buffer = GetBufferPoolManager(page_id);
  return buffer->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(size_t i = 0; i < num_instances_; i++) {
      buffers_[i]->FlushAllPgsImp();
  }
}

}  // namespace bustub
