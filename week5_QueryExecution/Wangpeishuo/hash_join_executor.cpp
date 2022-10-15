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

#include "hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_child_(std::move(left_child)),
    right_child_(std::move(right_child)){}

void HashJoinExecutor::Init()
{
    left_child_->Init();
    right_child_->Init();

    Tuple tp;
    RID rid;
    while (left_child_->Next(&tp, &rid))
    {
        jht_.Insert(MakeHashKey(&tp, 1),tp);
    }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
    if (!res.empty())
    {
        *tuple = res.front();
        *rid = tuple->GetRid();
        res.pop();
        return true;
    }

    if (!right_child_->Next(tuple, rid)) return false;

    if (jht_.Count(MakeHashKey(tuple, 2)) == 0) //没有相应的桶！
    {
        return Next(tuple, rid);
    }
    else
    {
        HashValue tuples = jht_.GetTuples(MakeHashKey(tuple, 2));
        for (int i = 0; i < tuples.same_tuples.size(); i++) //遍历所有的tuple
        {
            std::vector<Value> values;
            for (auto it : GetOutputSchema()->GetColumns())
            {
                values.push_back(it.GetExpr()->EvaluateJoin(&tuples.same_tuples[i], left_child_->GetOutputSchema(), tuple, right_child_->GetOutputSchema()));
            }
            res.push(Tuple(values, GetOutputSchema()));
        }
        *tuple = res.front();
        *rid = tuple->GetRid();
        res.pop();
        return true;
    }
}

}  // namespace bustub
