//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_executor_(std::move(left_executor)),
    right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init()
{
    left_executor_->Init();
    right_executor_->Init();
    right_over = true;//初始化为true，这样才可以让第一次使用NEXT时让左孩子先返回tuple
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool
{ 
    Tuple lt, rt;
    RID lr, rr;
    if (right_over) //需要重新拿一个左边的tuple
    {
        if (!left_executor_->Next(&lt, &lr)) //左边没了
            return false;
        else //拿到了，赶紧保存
        {
            ltuple = &lt;
            lrid = &lr;
            right_over = false;
            right_executor_->Init(); //右边重置
        }
    }
    while (1)
    {
        //下面需要让右边一直吐
        if (!right_executor_->Next(&rt, &rr)) //如果一上来右边就没了，左边还得再吐一个
        {
            if (!left_executor_->Next(&lt, &lr)) //左边没了
                return false;
            else //拿到了，赶紧保存
            {
                ltuple = &lt;
                lrid = &lr;
                right_over = false;
                right_executor_->Init(); //右边重置
                right_executor_->Next(&rt, &rr);
            }
        }
        //如果到这了，说明已经读到了，可以连表了
        if (plan_->Predicate() == nullptr || plan_->Predicate()->EvaluateJoin(ltuple, left_executor_->GetOutputSchema(),
            &rt, right_executor_->GetOutputSchema()).GetAs<bool>())
        {
            std::vector<Value> values;
            for (auto col : GetOutputSchema()->GetColumns())
            {
                values.push_back(col.GetExpr()->EvaluateJoin(ltuple, left_executor_->GetOutputSchema(),
                    &rt, right_executor_->GetOutputSchema()));
            }
            *tuple = Tuple(values, GetOutputSchema());
            return true;
        }
    }

}

}  // namespace bustub
