//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_(&exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->schema_),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      iter_(table_heap_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_heap_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == table_heap_->End()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();

  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!lock_mgr->LockShared(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  //根据select字段生成对应的tuple
  std::vector<Value> values;
  for (size_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, schema_));
  }

  *tuple = Tuple(values, plan_->OutputSchema());

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (!lock_mgr->Unlock(txn, *rid)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    }
  }

  ++iter_;

  //判断tuple是否满足predicate条件
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict != nullptr && !predict->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
    return Next(tuple, rid);
  }

  return true;
}
}  // namespace bustub

