//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t table_id = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);
  txn_ = exec_ctx_->GetTransaction();
  TableHeap *table_heap = table_info_->table_.get();
  iterater_ = table_heap->Begin(txn_);
  iter_end_ = table_heap->End();
}

Tuple SeqScanExecutor::GenerateTuple(Tuple *tuple, const Schema &schema) {
  std::vector<Value> values;
  for (auto const &col : schema.GetColumns()) {
    values.emplace_back(col.GetExpr()->Evaluate(tuple, &table_info_->schema_));
  }
  return Tuple{values, &schema};
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  assert(plan_ != nullptr);
  while (iterater_ != iter_end_) {
    *tuple = *iterater_;
    iterater_++;
    *rid = tuple->GetRid();
    const Schema *output_schema = plan_->OutputSchema();
    LockManager *lock_mgr = exec_ctx_->GetLockManager();
    if (plan_->GetPredicate() == nullptr || plan_->GetPredicate()->Evaluate(tuple, output_schema).GetAs<bool>()) {
      switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
        case IsolationLevel::READ_UNCOMMITTED:
          break;
        case IsolationLevel::READ_COMMITTED:
          if (!lock_mgr->IsLockShared(txn_, *rid) && !lock_mgr->IsLockExclusive(txn_, *rid)) {
            lock_mgr->LockShared(txn_, tuple->GetRid());
            lock_mgr->Unlock(txn_, tuple->GetRid());
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (!lock_mgr->IsLockShared(txn_, *rid) && !lock_mgr->IsLockExclusive(txn_, *rid)) {
            // LOG_DEBUG("lock on share mode, transaction id is %d", txn_->GetTransactionId());
            // std::cout << *rid << "\n";
            lock_mgr->LockShared(txn_, tuple->GetRid());
          }
          break;
      }
      *tuple = GenerateTuple(tuple, *output_schema);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
