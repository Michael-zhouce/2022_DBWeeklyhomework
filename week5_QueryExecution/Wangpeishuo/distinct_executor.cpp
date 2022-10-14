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

#include "distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    dht_iterator_(dht_.Begin())
    {}

void DistinctExecutor::Init()
{
    child_executor_->Init();
    Tuple tp;
    RID rid;
    while (child_executor_->Next(&tp, &rid)) //Init阶段其实已经弄完了
    {
        dht_.Insert(MakeDistinctKey(&tp), tp);
    }
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool
{ 
    if (dht_iterator_ == dht_.End())
    {
        return false;
    }
    *tuple = dht_iterator_.Val();
    *rid = tuple->GetRid();
    ++dht_iterator_;
    return true;
}

}  // namespace bustub
