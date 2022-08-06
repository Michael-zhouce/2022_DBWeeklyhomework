//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <climits>
#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPage(key, &root_is_latch);
  if (leaf_page == nullptr) {
    if (root_is_latch) {
      root_latch_.unlock();
      root_is_latch = false;
    }
    return false;
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType insert_value;
  bool found = leaf_node->Lookup(key, &insert_value, comparator_);
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  if (!found) {
    // DEBUG
    std::cout << "key " << key << "not found\n";
    return false;
  }
  // std::cout << "found, value is:" << insert_value;
  result->push_back(insert_value);
  if (root_is_latch) {
    root_latch_.unlock();
    root_is_latch = false;
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  {
    const std::lock_guard<std::mutex> guard(root_latch_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LOG_DEBUG("start new tree");
  page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&page_id);
  if (new_page == nullptr) {
    throw Exception("out of memory");
    return;
  }
  root_page_id_ = page_id;
  UpdateRootPageId(1);  // It doesn't exists, so we should insert instead of updating.
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPageByOperation(key, &root_is_latch, HowToGetLeaf::SEARCH, Operation::INSERT, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int size = leaf_node->GetSize();
  int new_size = leaf_node->Insert(key, value, comparator_);
  if (new_size == size) {
    if (root_is_latch) {
      root_latch_.unlock();
      root_is_latch = false;
    }
    UnlockUnpinPages(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // There is no need to split.
  if (new_size < leaf_node->GetMaxSize()) {
    if (root_is_latch) {
      root_latch_.unlock();
      root_is_latch = false;
    }
    // UnlockUnpinPages(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
  }

  // should split.
  std::cout << "split, reach max size:" << leaf_node->GetMaxSize() << ", cur size:" << leaf_node->GetSize() << "\n";
  LeafPage *new_node = Split<LeafPage>(leaf_node);
  LeafPage *old_node = leaf_node;
  InsertIntoParent(old_node, new_node->KeyAt(0), new_node, transaction, &root_is_latch);
  if (root_is_latch) {
    root_latch_.unlock();
    root_is_latch = false;
  }
  // UnlockPages(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception("out of memory");
    return nullptr;
  }
  N *ret = nullptr;
  if (node->IsLeafPage()) {
    LeafPage *old_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_node->Init(new_page_id, old_node->GetParentPageId(), leaf_max_size_);
    old_node->MoveHalfTo(new_node);
    new_node->SetNextPageId(old_node->GetNextPageId());
    old_node->SetNextPageId(new_node->GetPageId());
    ret = reinterpret_cast<N *>(new_node);
  } else {
    InternalPage *old_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_node->Init(new_page_id, old_node->GetParentPageId(), internal_max_size_);
    old_node->MoveHalfTo(new_node, buffer_pool_manager_);
    ret = reinterpret_cast<N *>(new_node);
  }
  return ret;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
// InsertIntoParent中root_latch_需要锁的话一定在InsertIntoLeaf中上锁了，在该函数中只管释放锁
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latch) {
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page = nullptr;
  InternalPage *parent_node = nullptr;
  std::cout << "insert into parent\n";
  // std::cout << "parent_page_Id:" << parent_page_id << "\n";
  // There only exists leaf nodes.
  if (old_node->IsRootPage()) {  // It's equal to old_node->IsRootPage();
    parent_page = buffer_pool_manager_->NewPage(&parent_page_id);
    parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_node->Init(parent_page_id, INVALID_PAGE_ID, internal_max_size_);
    if (parent_page == nullptr) {
      throw Exception("out of memory");
      return;
    }

    root_page_id_ = parent_page_id;
    parent_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(parent_page_id);
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    UpdateRootPageId(0);
    if (*root_is_latch) {
      root_latch_.unlock();
      *root_is_latch = false;
    }
    UnlockPages(transaction);
    return;
  }

  parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // std::cout << "parent_node size before:" << parent_node->GetSize() << "\n";
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // std::cout << "parent_node size after:" << parent_node->GetSize() << "\n";
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    if (*root_is_latch) {
      *root_is_latch = false;
      root_latch_.unlock();
    }
    UnlockPages(transaction);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }

  // recursively split
  InternalPage *new_parent_node = Split<InternalPage>(parent_node);
  // std::cout << "key is:" << new_parent_node->KeyAt(0) << "\n";
  InternalPage *old_parent_node = parent_node;
  InsertIntoParent(old_parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction, root_is_latch);
  UnlockPages(transaction);
  buffer_pool_manager_->UnpinPage(old_parent_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::cout << "remove key:" << key << "\n";
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPageByOperation(key, &root_is_latch, HowToGetLeaf::SEARCH, Operation::DELETE, transaction);
  if (leaf_page == nullptr) {
    if (root_is_latch) {
      root_latch_.unlock();
    }
    return;
  }
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  assert(leaf_node != nullptr);
  int leaf_node_size = leaf_node->GetSize();
  int size_after_delete = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  if (leaf_node_size == size_after_delete) {  // Deletion failed
    if (root_is_latch) {
      root_latch_.unlock();
      root_is_latch = false;
    }
    UnlockUnpinPages(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return;
  }

  LOG_DEBUG("leaf node page id:%d", leaf_node->GetPageId());
  CoalesceOrRedistribute(leaf_node, transaction, &root_is_latch);
  if (root_is_latch) {
    root_latch_.unlock();
    root_is_latch = false;
  }

  UnlockPages(transaction);
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    LOG_DEBUG("delete page, page id is:%d", page_id);
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
// 先向左边兄弟借，不够就和左边的兄弟合并。如果没有左兄弟，就向右兄弟借，否则就跟右兄弟合并。
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latch) {
  LOG_DEBUG("CoalesceOrRedistribute, page id is:%d", node->GetPageId());
  if (node->IsRootPage()) {
    LOG_DEBUG("node id is:%d", node->GetPageId());
    bool node_should_delete = AdjustRoot(node);
    if (node_should_delete) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    if (*root_is_latch) {
      root_latch_.unlock();
      *root_is_latch = false;
    }
    return node_should_delete;
  }

  if (node->GetSize() >= node->GetMinSize()) {
    if (*root_is_latch) {
      root_latch_.unlock();
      *root_is_latch = false;
    }
    UnlockPages(transaction);
    return false;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent_node->ValueIndex(node->GetPageId());
  int sibling_page_id = parent_node->ValueAt(index == 0 ? 1 : index - 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  sibling_page->WLatch();
  N *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());
  LOG_DEBUG("sibling size:%d, node size:%d, max size:%d", sibling_node->GetSize(), node->GetSize(), node->GetMaxSize());
  if (sibling_node->GetSize() + node->GetSize() >= node->GetMaxSize()) {  // just borrow key from sibling
    if (*root_is_latch) {
      root_latch_.unlock();
      *root_is_latch = false;
    }
    Redistribute(sibling_node, node, index);
    UnlockPages(transaction);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return false;
  }

  // Should merge node
  Coalesce(&sibling_node, &node, &parent_node, index, transaction, root_is_latch);
  LOG_DEBUG("merge node, page id is:%d, size is:%d", node->GetPageId(), node->GetSize());
  // if (parent_should_delete) {
  //   LOG_DEBUG("add into delete page set, page id is:%d", parent_page->GetPageId());
  //   transaction->AddIntoDeletedPageSet(parent_page->GetPageId());
  // }
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  return true;  // Merge means that move node's all keys to sibling, and node should be deleted.
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latch) {
  // Assume that neighbor_node is on node's left
  if (index == 0) {
    std::swap(neighbor_node, node);
    index = 1;
  }
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    LOG_DEBUG("leaf node page id:%d, sibling node page id:%d", leaf_node->GetPageId(), leaf_neighbor_node->GetPageId());
    leaf_node->MoveAllTo(leaf_neighbor_node);
    leaf_neighbor_node->SetNextPageId(leaf_node->GetNextPageId());
  } else {
    KeyType middle_key = (*parent)->KeyAt(index);
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(internal_neighbor_node, middle_key, buffer_pool_manager_);
  }

  (*parent)->Remove(index);
  transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  return CoalesceOrRedistribute(*parent, transaction, root_is_latch);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // Assume that It must be able to borrow key-value pair from sibiling in this function.
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (node->IsLeafPage()) {  // Sibling and me are both leaf nodes
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {  // sibiling is on my right
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
      parent_node->SetKeyAt(1, leaf_neighbor_node->KeyAt(0));
    } else {  // sibling is on my left
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
      parent_node->SetKeyAt(index, leaf_node->KeyAt(0));
    }
  } else {  // Sibling and me are both internal nodes
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *internel_neighbor_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {  // sibiling is on my right
      internel_neighbor_node->MoveFirstToEndOf(internal_node, parent_node->KeyAt(1), buffer_pool_manager_);
      parent_node->SetKeyAt(1, internel_neighbor_node->KeyAt(0));
    } else {  // sibling is on my left
      internel_neighbor_node->MoveLastToFrontOf(internal_node, parent_node->KeyAt(index), buffer_pool_manager_);
      parent_node->SetKeyAt(index, internal_node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  assert(old_root_node != nullptr);
  // case 1
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    LOG_DEBUG("case 1");
    InternalPage *root_internal_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t child_page_id = root_internal_node->RemoveAndReturnOnlyChild();
    LOG_DEBUG("root page id:%d, child page id:%d", root_page_id_, child_page_id);
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child_page_id, true);
    return true;
  }

  // case 2
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  // no deletion happend
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  std::cout << "begin";
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPage(KeyType(), &root_is_latch, HowToGetLeaf::LEFT_MOST);  // find leafmost leaf page
  INDEXITERATOR_TYPE iter(buffer_pool_manager_, leaf_page, 0);
  return iter;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  std::cout << "Begin\n";
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPage(key, &root_is_latch);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  bool root_is_latch = false;
  Page *leaf_page = FindLeafPage(KeyType(), &root_is_latch, HowToGetLeaf::RIGHT_MOST);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool *root_is_latch, HowToGetLeaf mode) {
  return FindLeafPageByOperation(key, root_is_latch, mode, Operation::FIND, nullptr);
}

/**
 * @brief Notes: all pages accessed are latched, but all pages except leaf_page are added into transaction.
 *
 * @param key
 * @param mode
 * @param op
 * @param transaction
 * @return INDEX_TEMPLATE_ARGUMENTS*
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, bool *root_is_latch, HowToGetLeaf mode, Operation op,
                                              Transaction *transaction) {
  // we have to use transaction to store pages being latched when
  // execute insertion or deletion
  if (op == Operation::INSERT || op == Operation::DELETE) {
    assert(transaction != nullptr);
  }

  bool allocated = false;
  if (root_is_latch == nullptr) {
    root_is_latch = new bool();
    allocated = true;
  }

  // protect root_page_id_ util we ensure root_page_id_ is no longer be changed
  root_latch_.lock();
  *root_is_latch = true;
  if (IsEmpty()) {
    *root_is_latch = false;
    root_latch_.unlock();
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page_id_t next_page_id;
  Page *cur_page = page;

  // accquire latch on the root page
  if (op == Operation::FIND) {
    page->RLatch();
    root_latch_.unlock();
    *root_is_latch = false;
  } else {
    page->WLatch();
    if (IsSafe(node, op)) {  // Ensure that root_page_id_ will not be changed
      root_latch_.unlock();
      *root_is_latch = false;
    }
  }

  // Find leaf page with loop. We should accquire different latched(Rlatch or Wlatch) for different operation,
  // i.e. GetValue(), Insert(), Remove().
  while (!node->IsLeafPage()) {
    InternalPage *cur_node = reinterpret_cast<InternalPage *>(node);

    // Fetch next page.
    if (mode == HowToGetLeaf::SEARCH) {
      next_page_id = cur_node->Lookup(key, comparator_);
    } else if (mode == HowToGetLeaf::LEFT_MOST) {
      next_page_id = cur_node->ValueAt(0);
    } else if (mode == HowToGetLeaf::RIGHT_MOST) {
      next_page_id = cur_node->ValueAt(cur_node->GetSize() - 1);
    } else {
      next_page_id = cur_node->Lookup(key, comparator_);
    }
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());

    // Accquire latch on the child node and release latch if child node is "safe"
    if (op == Operation::FIND) {
      next_page->RLatch();
      cur_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    } else {
      next_page->WLatch();
      transaction->AddIntoPageSet(cur_page);  // Page sets being latched
      if (IsSafe(next_node, op)) {
        if (*root_is_latch) {
          root_latch_.unlock();
          *root_is_latch = false;
        }
        UnlockUnpinPages(transaction);
      }
    }
    cur_page = next_page;
    node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }
  if (allocated) {
    delete root_is_latch;
    root_is_latch = nullptr;
  }
  return cur_page;
}

/**
 * @brief Unlock pages that are latchted
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPages(Transaction *transaction) {
  assert(transaction != nullptr);
  for (Page *page : *(transaction->GetPageSet())) {
    page->WUnlatch();
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  assert(transaction != nullptr);
  for (Page *page : *(transaction->GetPageSet())) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafe(BPlusTreePage *node, Operation op) {
  bool result = false;
  if (node->IsRootPage()) {
    result = (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
             (op == Operation::DELETE && node->GetSize() > 2);
  } else if (op == Operation::FIND) {
    result = true;
  } else if (op == Operation::INSERT) {
    result = node->GetSize() < node->GetMaxSize() - 1;
  } else if (op == Operation::DELETE) {
    result = node->GetSize() > node->GetMinSize();
  }
  return result;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
