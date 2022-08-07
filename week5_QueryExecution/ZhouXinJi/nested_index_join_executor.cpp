//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
}

Tuple NestIndexJoinExecutor::CombineTuples(Tuple *outer_tuple, Tuple *inner_tuple) {
  std::vector<Value> vals;
  for (auto const &col : plan_->OutputSchema()->GetColumns()) {
    vals.emplace_back(
        col.GetExpr()->EvaluateJoin(outer_tuple, plan_->OuterTableSchema(), inner_tuple, plan_->InnerTableSchema()));
  }
  return Tuple{vals, plan_->OutputSchema()};
}
bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    Tuple inner_tuple;
    RID inner_rid;
    if (!inner_matched_tuple_rid_list_.empty()) {
      inner_rid = inner_matched_tuple_rid_list_.back();
      inner_matched_tuple_rid_list_.pop_back();
      inner_table_info_->table_->GetTuple(inner_rid, &inner_tuple, exec_ctx_->GetTransaction());
      *tuple = CombineTuples(&outer_tuple_, &inner_tuple);
      return true;
    }
    if (!child_executor_->Next(&outer_tuple_, rid)) {
      return false;
    }
    // 将outer tuple转换成inner index的key
    Value key_value = plan_->Predicate()->GetChildAt(0)->EvaluateJoin(&outer_tuple_, plan_->OuterTableSchema(),
                                                                      &inner_tuple, &(inner_table_info_->schema_));
    Tuple probe_key = Tuple{{key_value}, inner_index_info_->index_->GetKeySchema()};
    inner_index_info_->index_->ScanKey(probe_key, &inner_matched_tuple_rid_list_, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
