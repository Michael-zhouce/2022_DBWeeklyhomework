/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, Page *page, int index) {
  buffer_pool_manager_ = buffer_pool_manager;
  cur_page_ = page;
  index_ = index;
  leaf_page_ = reinterpret_cast<LeafPage *>(cur_page_->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  cur_page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) && (index_ == leaf_page_->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw Exception("iterator is at the end!");
  }
  return leaf_page_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  std::cout << "cur index:" << index_ << ",cur page id:" << leaf_page_->GetPageId() << "\n";
  index_++;
  if (index_ >= leaf_page_->GetSize() && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    Page *next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
    cur_page_->RUnlatch();
    next_page->RLatch();
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    cur_page_ = next_page;
    leaf_page_ = reinterpret_cast<LeafPage *>(cur_page_->GetData());
    index_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf_page_->GetPageId() == itr.leaf_page_->GetPageId() && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { return !(*this == itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
