//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      dht_iterator_(dht_.Begin()) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tmp_tuple;
  RID tmp_rid;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    dht_.Insert(MakeDistinctKey(&tmp_tuple), tmp_tuple);
  }
  dht_iterator_ = dht_.Begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (dht_iterator_ == dht_.End()) {
    return false;
  }
  *tuple = dht_iterator_.Val();
  *rid = tuple->GetRid();
  ++dht_iterator_;
  return true;
}

}  // namespace bustub
