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
    right_over = true;//��ʼ��Ϊtrue�������ſ����õ�һ��ʹ��NEXTʱ�������ȷ���tuple
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool
{ 
    Tuple lt, rt;
    RID lr, rr;
    if (right_over) //��Ҫ������һ����ߵ�tuple
    {
        if (!left_executor_->Next(&lt, &lr)) //���û��
            return false;
        else //�õ��ˣ��Ͻ�����
        {
            ltuple = &lt;
            lrid = &lr;
            right_over = false;
            right_executor_->Init(); //�ұ�����
        }
    }
    while (1)
    {
        //������Ҫ���ұ�һֱ��
        if (!right_executor_->Next(&rt, &rr)) //���һ�����ұ߾�û�ˣ���߻�������һ��
        {
            if (!left_executor_->Next(&lt, &lr)) //���û��
                return false;
            else //�õ��ˣ��Ͻ�����
            {
                ltuple = &lt;
                lrid = &lr;
                right_over = false;
                right_executor_->Init(); //�ұ�����
                right_executor_->Next(&rt, &rr);
            }
        }
        //��������ˣ�˵���Ѿ������ˣ�����������
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
