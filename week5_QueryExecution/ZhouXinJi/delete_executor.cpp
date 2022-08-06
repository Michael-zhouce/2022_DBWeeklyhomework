//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_oid_t table_id = plan_->TableOid();
  table_info_ = exec_ctx->GetCatalog()->GetTable(table_id);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Get tuple from child executor.
  Tuple delete_tuple;
  if (!child_executor_->Next(&delete_tuple, rid)) {
    return false;
  }

  // Delete tuple from table heap.
  bool deleted = table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
  if (!deleted) {
    return false;
  }

  // Remember to delete index as well.
  for (auto &index_info : index_infos) {
    Tuple key =
        delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    RID value = *rid;
    index_info->index_->DeleteEntry(key, value, exec_ctx_->GetTransaction());
  }
  return deleted;
}

}  // namespace bustub
