//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t table_id = plan_->TableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(table_id);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Get tuple from child executor whose rid is valid.
  Tuple dummpy_tuple;
  if (!child_executor_->Next(&dummpy_tuple, rid)) {
    return false;
  }

  // Fetch tuple from table heap.
  Tuple old_tuple;
  bool fetched = table_info_->table_->GetTuple(*rid, &old_tuple, exec_ctx_->GetTransaction());
  if (!fetched) {
    return false;
  }

  // Generate updated_tuple and update table heap.
  Tuple updated_tuple = GenerateUpdatedTuple(old_tuple);
  bool updated = table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction());

  // update indexs
  if (updated) {
    for (auto index_info : index_infos) {
      Tuple key =
          updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      RID value = *rid;
      index_info->index_->DeleteEntry(key, value, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(key, value, exec_ctx_->GetTransaction());
    }
  }
  return updated;
}

}  // namespace bustub
