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

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
    bool flag = false;
    for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {  
        if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
            result->push_back(array_[i].second);      //当前位置可读且数据匹配，则获取相应的值
            flag = true;
        }
    }
    return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
    if (!IsReadable(i)) {                               //若当前位置不可读(即为空或被墓碑标记)，则尝试插入
        if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
            return false;                              //已存在目标键值对
        }
        SetReadable(i);
        SetOccupied(i);
        array_[i] = MappingType(key, value);
        return true;
    }
    return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i ++) {
        if (IsReadable(i)) {                            //若当前位置可读(即非空且非墓碑标记)，则进行键值匹配
            if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
                uint32_t byte_idx = i / 8;
                uint32_t inter_idx = i % 8;
                readable_[byte_idx] = readable_[byte_idx] & ~(1 << inter_idx);  //置为不可读(即添加墓碑标记)
                return true;
            }
        }
    }
    return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
    return array_[bucket_idx].first;            //获取当前位置对应的key
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
    return array_[bucket_idx].second;           //获取当前位置对应的value
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
    //occupied_[]以字节为单位,长度为array_[]的1/8,每个字节的每个bit对应array_[]中的索引位
    uint32_t byte_idx = bucket_idx / 8;         //计算array_[]中的索引对应到occupied_[]中的字节索引
    uint32_t inter_idx = bucket_idx % 8;        //计算occupied_[]中对应字节内部的bit位索引
    return (occupied_[byte_idx] >> inter_idx) & 1;  //按位移动构成 xxxxi & 00001 的形式判断第i位是否被占用
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
    uint32_t byte_idx = bucket_idx / 8;
    uint32_t inter_idx = bucket_idx % 8;
    occupied_[byte_idx] = occupied_[byte_idx] | (1 << inter_idx);
    //利用与运算构成 xxixxx | 001000 的形式将第i位置为1
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
    //readable_[]以字节为单位,长度为array_[]的1/8,每个字节的每个bit对应array_[]中的索引位
    uint32_t byte_idx = bucket_idx / 8;         //操作方式同occupied_[]
    uint32_t inter_idx = bucket_idx % 8;
    return (readable_[byte_idx] >> inter_idx) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
    uint32_t byte_idx = bucket_idx / 8;
    uint32_t inter_idx = bucket_idx % 8;
    readable_[byte_idx] = readable_[byte_idx] | (1 << inter_idx);
    //利用与运算构成 xxixxx | 001000 的形式将第i位置为1
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return false;
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
