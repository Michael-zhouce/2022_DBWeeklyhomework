//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int low = 0;
  int high = GetSize() - 1;
  int middle;
  while (low <= high) {
    middle = low + (high - low) / 2;
    if (comparator(key, KeyAt(middle)) == 1) {  // "key" is greater than the middle
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }
  return low;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // std::cout << "insert key:" << key << "\n";
  int cur_size = GetSize();
  // 防止直接先插入0
  if (cur_size == 0) {
    array[0] = MappingType{key, value};
    SetSize(1);
    return 1;
  }
  int insert_index = KeyIndex(key, comparator);
  int last_key_index = cur_size - 1;
  // Key already exists
  if (comparator(KeyAt(insert_index), key) == 0) {
    return cur_size;
  }
  for (int i = last_key_index; i >= insert_index; --i) {
    array[i + 1] = array[i];
  }
  array[insert_index] = MappingType{key, value};
  SetSize(cur_size + 1);
  return cur_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int cur_size = GetSize();
  int source_index = cur_size / 2;
  int transfer_num = cur_size - source_index;
  recipient->CopyNFrom(array + source_index, transfer_num);
  SetSize(cur_size - transfer_num);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  // int cur_size = GetSize();
  // int source_index = 0;
  // while (source_index < size) {
  //   array[cur_size++] = items[source_index++];
  // }
  // SetSize(cur_size + size);
  int cur_size = GetSize();
  std::copy(items, items + size, array + cur_size);
  SetSize(cur_size + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
  bool found = false;
  int low = 0;
  int high = GetSize() - 1;
  int midlle;
  while (low <= high) {
    midlle = low + (high - low) / 2;
    if (comparator(key, KeyAt(midlle)) == -1) {
      high = midlle - 1;
    } else if (comparator(key, KeyAt(midlle)) == 1) {
      low = midlle + 1;
    } else {
      *value = array[midlle].second;
      found = true;
      break;
    }
  }
  return found;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  bool found = false;
  int low = 0;
  int high = GetSize() - 1;
  int midlle;
  int cur_size = GetSize();
  // binary search to find "key" index
  while (low <= high) {
    midlle = low + (high - low) / 2;
    if (comparator(key, KeyAt(midlle)) == -1) {
      high = midlle - 1;
    } else if (comparator(key, KeyAt(midlle)) == 1) {
      low = midlle + 1;
    } else {
      found = true;
      break;
    }
  }

  // not found
  if (!found) {
    return cur_size;
  }

  // found and delete
  for (int i = midlle; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  SetSize(cur_size - 1);
  return cur_size - 1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int cur_size = GetSize();
  recipient->CopyNFrom(array, cur_size);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  int cur_size = GetSize();
  recipient->CopyLastFrom(array[0]);
  for (int i = 0; i < cur_size - 1; i++) {
    array[i] = array[i + 1];
  }
  SetSize(cur_size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  int cur_size = GetSize();
  array[cur_size++] = item;
  SetSize(cur_size);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int cur_size = GetSize();
  int last_item_index = cur_size - 1;
  recipient->CopyFirstFrom(array[last_item_index]);
  SetSize(cur_size - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  int cur_size = GetSize();
  for (int i = cur_size; i > 0; --i) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  SetSize(cur_size + 1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
