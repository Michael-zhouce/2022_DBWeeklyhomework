//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()},
      aht_iterator_{aht_.Begin()} {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

Tuple AggregationExecutor::GenerateTuple() {
  std::vector<Value> vals;
  for (auto const &col : GetOutputSchema()->GetColumns()) {
    // 实际上就是把聚集的结果取出来再组成一个tuple
    vals.emplace_back(
        col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
  }
  return Tuple{vals, GetOutputSchema()};
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()
            ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
            .GetAs<bool>()) {
      *tuple = GenerateTuple();
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
}

}  // namespace bustub
