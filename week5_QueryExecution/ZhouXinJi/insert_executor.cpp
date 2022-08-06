//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_ptr_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  table_meta_data_ptr_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ptr_ = table_meta_data_ptr_->table_.get();
  indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_meta_data_ptr_->name_);
  if (plan_->IsRawInsert()) {
    iter_ = plan_->RawValues().begin();
  } else {
    child_executor_ptr_->Init();
  }
}

/**
 * @brief Insert tuple into tableheap and create indexs.
 *
 * @param tuple
 * @param rid
 */
void InsertExecutor::InsertTuple(Tuple *tuple, RID *rid) {
  table_heap_ptr_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  for (auto &index_info : indexs_) {
    Tuple key =
        tuple->KeyFromTuple(table_meta_data_ptr_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    RID value = *rid;
    index_info->index_->InsertEntry(key, value, exec_ctx_->GetTransaction());
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (!plan_->IsRawInsert()) {
    bool success = child_executor_ptr_->Next(tuple, rid);
    if (success) {
      InsertTuple(tuple, rid);
    }
    return success;
  }

  // raw insert which means tuples are from InsertPlan
  if (iter_ != plan_->RawValues().end()) {
    Tuple tuple(*iter_, &table_meta_data_ptr_->schema_);
    InsertTuple(&tuple, rid);
    iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
