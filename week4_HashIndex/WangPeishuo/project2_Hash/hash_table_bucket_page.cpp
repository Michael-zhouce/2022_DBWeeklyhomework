//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "hash_table_bucket_page.h"
#include "logger.h"
#include "hash_util.h"
#include "generic_key.h"
#include "hash_comparator.h"
#include "tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool
{
    bool found = false;  //是否至少找到一个
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (IsReadable(i) && cmp(key, array_[i].first) == 0)
        {
            found = true;
            result->push_back(array_[i].second);
        }
    }
    return found;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool
{
    //先遍历一次，如果有重复的就不插入了，直接返回false
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second)
            return false;
    }
    //再遍历一次，找地方
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (!IsReadable(i))
        {
            SetOccupied(i);
            SetReadable(i);
            /*array_[i].first = key;
            array_[i].second = value;*/
            array_[i] = MappingType(key, value);
            return true;
        }
    }
    return false; //有可能满了，所以要加上这句
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool
{
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second)
        {
            RemoveAt(i);
            return true;
        }
    }
    return false; //没找到
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType
{
    return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType
{
    return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx)
{
    //readable置为0就可以了
    readable_[bucket_idx / 8] = readable_[bucket_idx / 8] & ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool
{
    if ((occupied_[i / 8] >> (i % 8) & 1) == 1) return true;
    return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx)
{
    //对应位，置1即可
    occupied_[bucket_idx / 8] = occupied_[bucket_idx / 8] | (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool
{
    if ((readable_[i / 8] >> (i % 8) & 1) == 1) return true;
    return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx)
{
    //对应位，置1即可
    readable_[bucket_idx / 8] = readable_[bucket_idx / 8] | (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool
{
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (!IsReadable(i))
        {
            return false;
        }
    }
    return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t
{
    uint32_t num;
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (IsReadable(i))
        {
            num++;
        }
    }
    return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool
{
    for (int i = 0; i < BUCKET_ARRAY_SIZE; i++)
    {
        if (IsReadable(i))
        {
            return false;
        }
    }
    return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
