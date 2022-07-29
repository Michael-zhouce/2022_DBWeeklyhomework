//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple left_tuple;
  RID left_rid;

  //把所有left_child_的tuple插入hashtable
  while (left_child_->Next(&left_tuple, &left_rid)) {
    jht_.Insert(MakeLeftHashJoinKey(&left_tuple), left_tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!tmp_results_.empty()) {
    *tuple = tmp_results_.front();
    *rid = tuple->GetRid();
    tmp_results_.pop();
    return true;
  }

  Tuple right_tuple;
  RID right_rid;

  //匹配right_tuple
  if (!right_child_->Next(&right_tuple, &right_rid)) {
    return false;
  }

  if (jht_.Count(MakeRightHashJoinKey(&right_tuple)) == 0) {
    return Next(tuple, rid);
  }

  std::vector<Tuple> left_tuples = jht_.Get(MakeRightHashJoinKey(&right_tuple));
  for (const auto &left_tuple : left_tuples) {
    std::vector<Value> output;
    for (const auto &col : GetOutputSchema()->GetColumns()) {
      output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(), &right_tuple,
                                                   right_child_->GetOutputSchema()));
    }
    tmp_results_.push(Tuple(output, GetOutputSchema()));
  }

  return Next(tuple, rid);
}

}  // namespace bustub
