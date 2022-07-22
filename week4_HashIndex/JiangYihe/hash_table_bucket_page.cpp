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
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool res = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {          // 遍历查找
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {  // 可读且匹配
      result->push_back(array_[i].second);                  // 得到value
      res = true;
    }
  }
  return res;  // 查找失败
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int64_t free_slot = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {  // 插入重复元素
        return false;
      }
    } else if (free_slot == -1) {
      free_slot = i;
    }
  }

  if (free_slot == -1) {
    // bucket已满
    return false;
  }

  // 将元素插入并且返回true
  SetOccupied(free_slot);                       // 表示该位置已经被占用，删除时，Occupied值不变
  SetReadable(free_slot);                       // 该位置已经插入有效kv对
  array_[free_slot] = MappingType(key, value);  // 插入新的kv对
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        RemoveAt(i);  // 删除
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;  // key值
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;  // value值
}

/* char类型数组,每个单位 8 bit,能标记8个位置是否有键值对存在, bucket_idx / 8找到应该修改的字节位置
   bucket_idx % 8 找出在该字节的对应的bit位（可理解为二维数组，bucket_idx / 8 找到哪一行
   bucket_idx % 8找到哪一列）
   构建一个8位长度的,除该位为0外,其他全为1的Byte,例如: bucket_idx=5,原数为10100011，想清除第5位
   bucket_idx % 8 = 5,00000001左移5位->00100000,按位取反->11011111
   然后与相应字节取按位与操作,则实现清除该位置的 1 的操作,而其他位置保持不变
   10100011 &
   11011111   -> 10000011，第5位清除成功*/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  size_t c = readable_[bucket_idx / 8];
  c = c & (~(1 << (bucket_idx % 8)));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

// 标记哪一位被占用过（1 & 1 = 1）
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  size_t c = occupied_[bucket_idx / 8];
  c = c & (1 << (bucket_idx % 8));
  return c != 0;
}

// 不影响其他位，标记位置为1
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  size_t c = occupied_[bucket_idx / 8];
  c = c | (1 << (bucket_idx % 8));
  occupied_[bucket_idx / 8] = static_cast<char>(c);
}

//标记位置1，表示该位有有效的kv对
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  size_t c = readable_[bucket_idx / 8];
  c = c & (1 << (bucket_idx % 8));
  return c != 0;
}

//设置标记位有有效的kv对
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  size_t c = readable_[bucket_idx / 8];
  c = c | (1 << (bucket_idx % 8));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {  // 判断是否满
  u_int8_t mask = 255;
  size_t times = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < times; i++) {
    char c = readable_[i];
    uint8_t ic = static_cast<uint8_t>(c);
    if ((ic & mask) != mask) {
      return false;
    }
  }

  size_t remain = BUCKET_ARRAY_SIZE % 8;
  if (remain > 0) {
    char c = readable_[times];
    uint8_t ic = static_cast<uint8_t>(c);
    for (size_t i = 0; i < remain; i++) {
      if ((ic & 1) != 1) {
        return false;
      }
      ic = ic >> 1;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {  // readable的数目
  uint32_t num = 0;
  size_t times = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < times; i++) {
    char c = readable_[i];
    uint8_t ic = static_cast<uint8_t>(c);
    for (uint32_t j = 0; j < 8; j++) {
      if ((ic & 1) > 0) {
        num++;
      }
      ic = ic >> 1;
    }
  }

  size_t remain = BUCKET_ARRAY_SIZE % 8;
  if (remain > 0) {
    char c = readable_[times];
    uint8_t ic = static_cast<uint8_t>(c);
    for (size_t i = 0; i < remain; i++) {
      if ((ic & 1) == 1) {
        num++;
      }
      ic = ic >> 1;
    }
  }
  return num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {  // 判断是否为空
  u_int8_t mask = 255;
  for (size_t i = 0; i < sizeof(readable_) / sizeof(readable_[0]); i++) {
    char c = readable_[i];
    if ((c & mask) > 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      copy[index++] = array_[i];
    }
  }
  return copy;
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

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::Clear() {
  LOG_DEBUG("clear");
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
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