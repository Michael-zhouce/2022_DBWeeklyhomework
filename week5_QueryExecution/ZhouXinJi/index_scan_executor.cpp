//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_oid_t index_id = plan_->GetIndexOid();
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(index_id);
  table_heap_ptr = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
  txn_ = exec_ctx_->GetTransaction();
  B_PLUS_TREE_INDEX_TYPE *b_plus_tree_index = static_cast<B_PLUS_TREE_INDEX_TYPE *>(index_info->index_.get());
  iterator_ = b_plus_tree_index->GetBeginIterator();
  iter_end_ = b_plus_tree_index->GetEndIterator();
}

Tuple IndexScanExecutor::GenerateTuple(const Tuple &tuple, const Schema &output_schema) {
  std::vector<Value> result;
  for (auto &col : output_schema.GetColumns()) {
    result.emplace_back(col.GetExpr()->Evaluate(&tuple, &output_schema));
  }
  return Tuple{result, &output_schema};
}
bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iterator_ != iter_end_) {
    *rid = (*iterator_).second;
    table_heap_ptr->GetTuple(*rid, tuple, txn_);
    if (plan_->GetPredicate()->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
      *tuple = GenerateTuple(*tuple, *(plan_->OutputSchema()));
      return true;
    }
  }
  return false;
}

}  // namespace bustub
