//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_(std::move(child)),
    aht_(SimpleAggregationHashTable(plan->GetAggregates(),plan_->GetAggregateTypes())),
    aht_iterator_(aht_.Begin())
{}

void AggregationExecutor::Init()
{
    child_->Init();
    Tuple tp;
    RID rid;
    while (child_->Next(&tp, &rid)) //在这里就全运算完了
    {
        AggregateKey key = MakeAggregateKey(&tp);
        AggregateValue value = MakeAggregateValue(&tp);
        aht_.InsertCombine(key, value);
    }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
    if (aht_iterator_ == aht_.End()) return false;
    AggregateKey key = aht_iterator_.Key();
    AggregateValue value = aht_iterator_.Val();
    ++aht_iterator_;

    if (plan_->GetHaving() == nullptr || plan_->GetHaving()->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>())
    {
        std::vector<Value> values;
        for (auto it : plan_->OutputSchema()->GetColumns())
        {
            values.push_back(it.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        return true;
    }
    else
    {
        return Next(tuple, rid);
    }
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
