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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "exception.h"
#include "logger.h"
#include "rid.h"
#include "extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
    //初始化directory_page_id_
    page_id_t tmp_page_id;
    res = (HashTableDirectoryPage*)buffer_pool_manager_->NewPage(&tmp_page_id);
    directory_page_id_ = tmp_page_id;
    res->SetPageId(directory_page_id_);
    //整一个bucket的页
    page_id_t bucket_page_id;
    buffer_pool_manager_->NewPage(&bucket_page_id);
    res->SetBucketPageId(0, bucket_page_id);
}

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
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

//这个函数是用来获得bucket_idx的，即Hash（key） & globaldepthmask
template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t
{
    return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t
{
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage *
{
    HashTableDirectoryPage* ret;

    //强制类型转换，得到的本质上是一个页里面装的内容而已
    ret = (HashTableDirectoryPage*)buffer_pool_manager_->FetchPage(directory_page_id_)->GetData();
    return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE *
{
    return (HASH_TABLE_BUCKET_TYPE*)buffer_pool_manager_->FetchPage(bucket_page_id);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool
{
    table_latch_.RLock(); //上读锁
    HashTableDirectoryPage* dir_page = FetchDirectoryPage();
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    HASH_TABLE_BUCKET_TYPE* bucket = FetchBucketPage(bucket_pid);
    if (bucket->GetValue(key, comparator_, result))
    {
        table_latch_.RUnlock();
        return true;
    }
    table_latch_.RUnlock();
    return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
{
    table_latch_.WLock();
    HashTableDirectoryPage* dir_page = FetchDirectoryPage();
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    HASH_TABLE_BUCKET_TYPE* bucket = FetchBucketPage(bucket_pid);
    if (bucket->Insert(key, value, comparator_))
    {
        table_latch_.WUnlock();
        return true;
    }
    table_latch_.WUnlock();
    return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
{
    table_latch_.WLock();
    HashTableDirectoryPage* dir_page = FetchDirectoryPage();
    uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
    uint32_t bucket_depth = dir_page->GetLocalDepth(bucket_index);

    //如果此时这个桶的深度恰好等于global_depth，那是需要全局扩容的
    if (bucket_depth == dir_page->GetGlobalDepth())
        dir_page->IncrGlobalDepth();

    page_id_t bucket_page_id = KeyToPageId(key, dir_page);            //得到这个bucket页面的id
    HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(bucket_page_id); //得到对应的页
    Page* spage = (Page*)FetchBucketPage(bucket_page_id);
    page_id_t brother_page_id;
    //创建他哥们的页
    Page* brother_page = buffer_pool_manager_->NewPage(&brother_page_id);

    dir_page->IncrLocalDepth(bucket_index);
    //得到它哥们的index
    uint32_t brother_index = dir_page->GetSplitImageIndex(bucket_index);
    //将两个桶的深度都变成一样的
    dir_page->SetLocalDepth(brother_index, dir_page->GetLocalDepth(bucket_index));
    //虽然刚才创建了一个新页是他哥们，但是目录本身并不知道，还得更新一下
    dir_page->SetBucketPageId(brother_index, brother_page_id);
    //可能还有其他槽指向这些桶，所以要把他们的local深度也变一下，这里用循环即可
    uint32_t size = dir_page->Size();
    uint32_t new_depth = dir_page->GetLocalDepth(brother_index);
    for (uint32_t i = 0; i < size; i++)
    {
        if (dir_page->GetBucketPageId(i) == bucket_index || dir_page->GetBucketPageId(i) == brother_index)
        {
            dir_page->SetLocalDepth(i, new_depth);
        }
    }
    //虽然解决了local深度的问题，但是形式上的分裂还没有完成，可能有本该指向它哥们的槽还在指向他
    for (uint32_t i = 0; i < size; i++)
    {
        if (dir_page->GetBucketPageId(i) == bucket_index && (i & dir_page->GetLocalDepthMask())==(brother_index) )
        {
            dir_page->SetBucketPageId(i, brother_page_id);
            dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
        }
    }

    uint32_t size = bucket_page->NumReadable();
    //将bucket清空，但是清空之前得保存数组
    MappingType* ary = bucket_page->array();
    bucket_page->Clear();
    //需要重新分别插入bucket里的KV对
    HASH_TABLE_BUCKET_TYPE* brother_bpage = FetchBucketPage(brother_page_id); //得到对应的页
    for (uint32_t i = 0; i < size; i++)
    {
        uint32_t aim_index = Hash(bucket_array[i].first) & dir_page->GetLocalDepthMask(bucket_index);
        if (dir_page->GetBucketPageId(aim_index) == bucket_page_id)
        {
            bucket_page->Insert(bucket_array[i].first, bucket_array[i].second, comparator_);
        }
        else
        {
            brother_bpage->Insert(bucket_array[i].first, bucket_array[i].second, comparator_);
        }
    }
    table_latch_.WUnlock();
    return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
{
    table_latch_.WLock();
    HashTableDirectoryPage* dir_page = FetchDirectoryPage();
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    HASH_TABLE_BUCKET_TYPE* bucket = FetchBucketPage(bucket_pid);

    if (bucket->Remove(key, value, comparator_))
    {
        //如果删了，还是需要判断一下是否是空的，然后需要Merge一下桶才行
        if (bucket->IsEmpty())
        {
            Merge(transaction, key, value);
        }
        table_latch_.WUnlock();
        return true;
    }
    table_latch_.WUnlock();
    return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value)
{
    table_latch_.WLock();
    HashTableDirectoryPage* dir_page = FetchDirectoryPage();
    uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);           //bucket的id
    uint32_t brother_index = dir_page->GetSplitImageIndex(bucket_index);  //他兄弟的bucket_idx
    page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);   //bucket对应的page的id
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);         //bucket的local_depth
    page_id_t brother_page_id = dir_page->GetBucketPageId(brother_index);//他兄弟的page的id

    // 2.只有当他们有相同的局部深度时，桶才能合并
    if (local_depth != dir_page->GetLocalDepth(brother_index))
    {
        table_latch_.WUnlock();
        return;
    }
    // 3.只有局部深度大于0的桶才能被合并
    if (local_depth == 0) 
    { //最小了，不能合并
        table_latch_.WUnlock();
        return;
    }
    HASH_TABLE_BUCKET_TYPE* target_page = FetchBucketPage(bucket_index);  //拿到bucket的页面
    // 1.只能合并空桶
    if (!target_page->IsEmpty())
    {
        table_latch_.WUnlock();
        return;
    }
    //接下来需要合并了，先删除这个页，因为已知这bucket是个空桶，情况比较简单，所以是可以删除的。
    buffer_pool_manager_->DeletePage(bucket_index);

    dir_page->SetBucketPageId(bucket_index, brother_index); //原本那个指向bucket的槽现在要指向它哥们
    dir_page->DecrLocalDepth(bucket_index);
    dir_page->DecrLocalDepth(brother_index); //两个槽都减少local深度，其实现在指向同一个桶了
    
    //其实还有剩下的指向原bucket的槽，也需要都指向它哥们，这里简单的用循环就可以实现了
    //还有一些原来就指向它哥们的槽，修改一下local_depth是必要的!
    uint32_t size = dir_page->Size();
    uint32_t new_depth = dir_page->GetLocalDepth(brother_index);
    for (uint32_t i = 0; i < size; i++)
    {
        if (dir_page->GetBucketPageId(i) == bucket_index)
        {
            dir_page->SetBucketPageId(i, brother_index);
            dir_page->SetLocalDepth(i, new_depth);
        }
        else if(dir_page->GetBucketPageId(i) == brother_index)
            dir_page->SetLocalDepth(i, new_depth);
    }
    //尝试缩小
    if(dir_page->CanShrink()) dir_page->DecrGlobalDepth();
    table_latch_.WUnlock();
    return;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
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
