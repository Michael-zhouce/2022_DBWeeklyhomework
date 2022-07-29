//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      table_heap_(table_info_->table_.get()),
      child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {

  Transaction *txn = GetExecutorContext()->GetTransaction();
  while (child_executor_->Next(tuple, rid)) {

    //调用TableHeap的MarkDelete删除对应的tuple
    table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction());

    //删除索引
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());

      txn->GetIndexWriteSet()->push_back(
          (IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog())));
    }
  }
  return false;
}

}  // namespace bustub