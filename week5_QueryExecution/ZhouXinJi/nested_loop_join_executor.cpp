//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_continue_ = left_executor_->Next(&left_tuple_, &left_tuple_rid);
}

Tuple NestedLoopJoinExecutor::CombineTuples(Tuple *left_tuple, Tuple *right_tuple) {
  std::vector<Value> values;
  assert(plan_ != nullptr);
  for (auto const &col : plan_->OutputSchema()->GetColumns()) {
    Value value = col.GetExpr()->EvaluateJoin(left_tuple, left_executor_->GetOutputSchema(), right_tuple,
                                              right_executor_->GetOutputSchema());
    values.emplace_back(value);
  }
  return Tuple{values, plan_->OutputSchema()};
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!left_continue_) {
      return false;
    }

    Tuple right_tuple;
    RID right_tuple_id;
    if (!right_executor_->Next(&right_tuple, &right_tuple_id)) {
      right_executor_->Init();
      left_continue_ = left_executor_->Next(&left_tuple_, &left_tuple_rid);
      continue;
    }

    // should combine left_tuple and right_tuple
    if (plan_ == nullptr || plan_->Predicate()
                                ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema())
                                .GetAs<bool>()) {
      *tuple = CombineTuples(&left_tuple_, &right_tuple);
      LOG_DEBUG("pass exam");
      return true;
    }
  }
}

}  // namespace bustub
