//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

// 用key的hash值和全局深度对应的全1的掩码按位与操作获得bucket index
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {  // 获取directory page
  HashTableDirectoryPage *res;

  init_lock_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    page_id_t tmp_page_id;
    res =
        reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_->NewPage(&tmp_page_id))->GetData());
    directory_page_id_ = tmp_page_id;
    res->SetPageId(directory_page_id_);
    assert(directory_page_id_ != INVALID_PAGE_ID);

    page_id_t bucket_page_id = INVALID_PAGE_ID;
    AssertPage(buffer_pool_manager_->NewPage(&bucket_page_id));
    res->SetBucketPageId(0, bucket_page_id);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  }
  init_lock_.unlock();

  assert(directory_page_id_ != INVALID_PAGE_ID);
  res = reinterpret_cast<HashTableDirectoryPage *>(
      AssertPage(buffer_pool_manager_->FetchPage(directory_page_id_))->GetData());
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {  // 获取bucket page
  return AssertPage(buffer_pool_manager_->FetchPage(bucket_page_id));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::RetrieveBucket(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
// 根据key获取其在bucket内的值
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();  // 读锁

  // 取directory页和key所在的bucket页
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->RLatch();

  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(bucket_page);
  bool res = bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  // unpin页面
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);  // 提前获取页面
  Page *page = FetchBucketPage(bucket_page_id);           // 取bucket页面
  page->WLatch();

  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  if (!bucket->IsFull()) {
    // bucket没满才能insert
    bool res = bucket->Insert(key, value, comparator_);  // 记录是否插入成功
    page->WUnlatch();

    // unpin页面
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return res;
  }

  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);  // 满了，进行split insert
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth >= MAX_BUCKET_DEPTH) {
    // split_bucket_depth比最大的bucket_depth大
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();

    return false;
  }

  // directory扩容
  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 复制split的bucket数据并且删掉起始页面.
  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  Page *split_page = FetchBucketPage(split_bucket_page_id);

  split_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = RetrieveBucket(split_page);

  MappingType *origin_array = split_bucket->GetArrayCopy();
  uint32_t origin_array_size = split_bucket->NumReadable();
  split_bucket->Clear();

  page_id_t image_bucket_page;
  HASH_TABLE_BUCKET_TYPE *image_bucket = RetrieveBucket(AssertPage(buffer_pool_manager_->NewPage(&image_bucket_page)));

  // 在获取image split bucket之前增加local depth
  dir_page->IncrLocalDepth(split_bucket_index);
  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));

  // 设置dir_page
  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page);

  // 将所有同一级的bucket设置为相同的local depth和page
  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }
  for (uint32_t i = split_image_bucket_index; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    if (i < diff) {
      break;
    }
  }
  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
  }

  // 再次尝试插入（split后）
  uint32_t mask = dir_page->GetLocalDepthMask(split_bucket_index);
  for (uint32_t i = 0; i < origin_array_size; i++) {
    MappingType tmp = origin_array[i];
    uint32_t target_bucket_index = Hash(tmp.first) & mask;
    page_id_t target_bucket_index_page = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_index_page == split_bucket_page_id || target_bucket_index_page == image_bucket_page);
    if (target_bucket_index_page == split_bucket_page_id) {
      assert(split_bucket->Insert(tmp.first, tmp.second, comparator_));
    } else {
      assert(image_bucket->Insert(tmp.first, tmp.second, comparator_));
    }
  }

  delete[] origin_array;
  split_page->WUnlatch();

  // unpin页面
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page, true));

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // 计算删除数据key的hash值。
  // 与global depth进行&运算，得到其bucket index
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);

  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  bool res = bucket->Remove(key, value, comparator_);  // 删除

  if (bucket->IsEmpty()) {  // 删除后的bucket为空，直接shrink
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, bucket_index);
    return res;
  }
  page->WUnlatch();

  // unpin页面
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t target_bucket_index) {
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (target_bucket_index >= dir_page->Size()) {
    table_latch_.WUnlock();
    return;
  }

  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    // local_depth=0，不能merge
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // bucket和其split image bucket的local depth不相等，不进行shrink
  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  Page *target_page = FetchBucketPage(target_bucket_page_id);
  target_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *target_bucket = RetrieveBucket(target_page);
  if (!target_bucket->IsEmpty()) {
    // bucket不是空的，直接返回
    target_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // 删除page
  target_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  // 遍历整个directory,将所有指向target bucket page的bucket全部重新指向split image bucket的page
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AssertPage(Page *page) {
  assert(page != nullptr);
  return page;
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
