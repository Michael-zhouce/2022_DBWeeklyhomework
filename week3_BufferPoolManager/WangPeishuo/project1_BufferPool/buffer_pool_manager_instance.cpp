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
    if (page_table_.count(page_id)) //��������page_id
    {
        frame_id = page_table_[page_id];
        Page& page = pages_[frame_id];
        disk_manager_->WritePage(page_id, page.data_);
        page.is_dirty_ = false; //�Ѿ�������ҳ��
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
    if (!free_list_.empty()) //���free_list���գ��������ҳ�һ���ط�����
    {
        frame_id = free_list_.front(); //��freelist��ͷ���ó�һ��frame_id��
        free_list_.pop_front();
    }
    else   //BufferPool���ˣ�������ҪLRUһ�²���
    {
        if (!replacer_->Victim(&frame_id))
        {
            std::cout << "NULL!" << std::endl;
            latch_.unlock();
            return nullptr;
        }
    }
    //frame_id�����ڳ����ĵط�
    if (pages_[frame_id].is_dirty_) //����
    {//pages_[frame_id]Ϊ��ҳ
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //д
    }
    *page_id = AllocatePage();

    page_table_.erase(pages_[frame_id].page_id_); //�ȸ��¹�ϣ��

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    page_table_[pages_[frame_id].page_id_] = frame_id;

    disk_manager_->WritePage(*page_id, pages_[frame_id].data_); //�Ӵ����ж�������
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
    
    if (page_table_.count(page_id)) //��������page_id
    {
        frame_id_t frame_id = page_table_[page_id];
        //׼��ʹ�����frame����ҪPIN
        replacer_->Pin(frame_id);
        pages_[frame_id].pin_count_++; //���ҳ��pin������
        latch_.unlock();
        return &pages_[frame_id]; //�����page�ĵ�ַ����
    }
    else   //���û�����page_id
    {
        frame_id_t frame_id;
        if (!free_list_.empty()) //���free_list���գ��������ҳ�һ���ط�����
        {
            frame_id = free_list_.front(); //��freelist��ͷ���ó�һ��frame_id��
            free_list_.pop_front(); 
        }
        else   //BufferPool���ˣ�������ҪLRUһ�²���
        {
            if (!replacer_->Victim(&frame_id))
            {
                std::cout << "NULL!" << std::endl;
                latch_.unlock();
                return nullptr;
            }
        }
        //�����Ѿ��ҵ���frame_id��������Ҫ���µ�page��ռ����ط���������Ҫ�ȿ�ԭҳ�Ƿ���
        if (pages_[frame_id].is_dirty_) //����
        {//pages_[frame_id]Ϊ��ҳ
            disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //д
        }
        //������ʽ�����滻
        page_table_.erase(pages_[frame_id].page_id_); //�ȸ��¹�ϣ��

        pages_[frame_id].page_id_ = page_id;
        pages_[frame_id].pin_count_ = 1;
        pages_[frame_id].is_dirty_ = false;
        page_table_[pages_[frame_id].page_id_] = frame_id;

        disk_manager_->ReadPage(page_id, pages_[frame_id].data_); //�Ӵ����ж�������
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
    //����ɾ�����frame�����
    if (pages_[frame_id].is_dirty_) //����
    {//pages_[frame_id]Ϊ��ҳ
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //д
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
    //if(is_dirty) disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_); //д
    pages_[frame_id].is_dirty_ = is_dirty; //ֻ�ı�һ���Ƿ���Ϳ����ˣ��������ʱ��ȥд��
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
