//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  skiped_ = 0;
  emited_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
    if (skiped_ < plan_->GetOffset()) {
      skiped_++;
      continue;
    }
    if (emited_ >= plan_->GetLimit()) {
      return false;
    }
    emited_++;
    return true;
  }
}

}  // namespace bustub
