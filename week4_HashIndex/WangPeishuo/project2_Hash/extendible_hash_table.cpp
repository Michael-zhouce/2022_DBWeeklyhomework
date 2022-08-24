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
    //��ʼ��directory_page_id_
    page_id_t tmp_page_id;
    res = (HashTableDirectoryPage*)buffer_pool_manager_->NewPage(&tmp_page_id);
    directory_page_id_ = tmp_page_id;
    res->SetPageId(directory_page_id_);
    //��һ��bucket��ҳ
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

//����������������bucket_idx�ģ���Hash��key�� & globaldepthmask
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

    //ǿ������ת�����õ��ı�������һ��ҳ����װ�����ݶ���
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
    table_latch_.RLock(); //�϶���
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

    //�����ʱ���Ͱ�����ǡ�õ���global_depth��������Ҫȫ�����ݵ�
    if (bucket_depth == dir_page->GetGlobalDepth())
        dir_page->IncrGlobalDepth();

    page_id_t bucket_page_id = KeyToPageId(key, dir_page);            //�õ����bucketҳ���id
    HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(bucket_page_id); //�õ���Ӧ��ҳ
    Page* spage = (Page*)FetchBucketPage(bucket_page_id);
    page_id_t brother_page_id;
    //���������ǵ�ҳ
    Page* brother_page = buffer_pool_manager_->NewPage(&brother_page_id);

    dir_page->IncrLocalDepth(bucket_index);
    //�õ������ǵ�index
    uint32_t brother_index = dir_page->GetSplitImageIndex(bucket_index);
    //������Ͱ����ȶ����һ����
    dir_page->SetLocalDepth(brother_index, dir_page->GetLocalDepth(bucket_index));
    //��Ȼ�ղŴ�����һ����ҳ�������ǣ�����Ŀ¼������֪�������ø���һ��
    dir_page->SetBucketPageId(brother_index, brother_page_id);
    //���ܻ���������ָ����ЩͰ������Ҫ�����ǵ�local���Ҳ��һ�£�������ѭ������
    uint32_t size = dir_page->Size();
    uint32_t new_depth = dir_page->GetLocalDepth(brother_index);
    for (uint32_t i = 0; i < size; i++)
    {
        if (dir_page->GetBucketPageId(i) == bucket_index || dir_page->GetBucketPageId(i) == brother_index)
        {
            dir_page->SetLocalDepth(i, new_depth);
        }
    }
    //��Ȼ�����local��ȵ����⣬������ʽ�ϵķ��ѻ�û����ɣ������б���ָ�������ǵĲۻ���ָ����
    for (uint32_t i = 0; i < size; i++)
    {
        if (dir_page->GetBucketPageId(i) == bucket_index && (i & dir_page->GetLocalDepthMask())==(brother_index) )
        {
            dir_page->SetBucketPageId(i, brother_page_id);
            dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
        }
    }

    uint32_t size = bucket_page->NumReadable();
    //��bucket��գ��������֮ǰ�ñ�������
    MappingType* ary = bucket_page->array();
    bucket_page->Clear();
    //��Ҫ���·ֱ����bucket���KV��
    HASH_TABLE_BUCKET_TYPE* brother_bpage = FetchBucketPage(brother_page_id); //�õ���Ӧ��ҳ
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
        //���ɾ�ˣ�������Ҫ�ж�һ���Ƿ��ǿյģ�Ȼ����ҪMergeһ��Ͱ����
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
    uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);           //bucket��id
    uint32_t brother_index = dir_page->GetSplitImageIndex(bucket_index);  //���ֵܵ�bucket_idx
    page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);   //bucket��Ӧ��page��id
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);         //bucket��local_depth
    page_id_t brother_page_id = dir_page->GetBucketPageId(brother_index);//���ֵܵ�page��id

    // 2.ֻ�е���������ͬ�ľֲ����ʱ��Ͱ���ܺϲ�
    if (local_depth != dir_page->GetLocalDepth(brother_index))
    {
        table_latch_.WUnlock();
        return;
    }
    // 3.ֻ�оֲ���ȴ���0��Ͱ���ܱ��ϲ�
    if (local_depth == 0) 
    { //��С�ˣ����ܺϲ�
        table_latch_.WUnlock();
        return;
    }
    HASH_TABLE_BUCKET_TYPE* target_page = FetchBucketPage(bucket_index);  //�õ�bucket��ҳ��
    // 1.ֻ�ܺϲ���Ͱ
    if (!target_page->IsEmpty())
    {
        table_latch_.WUnlock();
        return;
    }
    //��������Ҫ�ϲ��ˣ���ɾ�����ҳ����Ϊ��֪��bucket�Ǹ���Ͱ������Ƚϼ򵥣������ǿ���ɾ���ġ�
    buffer_pool_manager_->DeletePage(bucket_index);

    dir_page->SetBucketPageId(bucket_index, brother_index); //ԭ���Ǹ�ָ��bucket�Ĳ�����Ҫָ��������
    dir_page->DecrLocalDepth(bucket_index);
    dir_page->DecrLocalDepth(brother_index); //�����۶�����local��ȣ���ʵ����ָ��ͬһ��Ͱ��
    
    //��ʵ����ʣ�µ�ָ��ԭbucket�Ĳۣ�Ҳ��Ҫ��ָ�������ǣ�����򵥵���ѭ���Ϳ���ʵ����
    //����һЩԭ����ָ�������ǵĲۣ��޸�һ��local_depth�Ǳ�Ҫ��!
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
    //������С
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
