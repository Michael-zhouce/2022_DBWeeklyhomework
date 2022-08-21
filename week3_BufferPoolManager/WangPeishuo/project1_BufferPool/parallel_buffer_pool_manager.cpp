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

#include "parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
{
    // Allocate and create individual BufferPoolManagerInstances
    num = num_instances;
    for (uint32_t i = 0; i < num_instances; i++) //����num_instances��Manager
    {
        BufferPoolManagerInstance m(pool_size, num_instances, i, disk_manager, log_manager);
        manages[i] = &m;
    }

}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager()
{
    for (uint32_t i = 0; i < num; i++) //����num_instances��Manager
    {
        free(manages[i]); //�ͷ�
    }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t
{
  // Get size of all BufferPoolManagerInstances
    return manages[0]->BufferPoolManagerInstance::GetPoolSize();
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManagerInstance*
{
    // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
    int x;
    x = page_id % num;
    return manages[x];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page *
{
    // Fetch page for page_id from responsible BufferPoolManagerInstance
    BufferPoolManagerInstance* bpmi = GetBufferPoolManager(page_id);
    return bpmi->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool
{
     // Unpin page_id from responsible BufferPoolManagerInstance
     BufferPoolManagerInstance* bpmi = GetBufferPoolManager(page_id);
     return bpmi->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool
{
    // Flush page_id from responsible BufferPoolManagerInstance
    BufferPoolManagerInstance* bpmi = GetBufferPoolManager(page_id);
    return bpmi->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page *
{
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
    Page* new_page = nullptr;

    for (uint32_t i = 0; i < num; i++)
    {
        new_page = manages[i]->NewPage(page_id);
        if (new_page != nullptr) return new_page;
    }
    return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool
{
    // Delete page_id from responsible BufferPoolManagerInstance
    BufferPoolManagerInstance* bpmi = GetBufferPoolManager(page_id);
    return bpmi->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp()
{
    for (uint32_t i = 0; i < num; i++)
    {
        manages[i]->FlushAllPages();
    }
    // flush all pages from all BufferPoolManagerInstances
}

}  // namespace bustub
