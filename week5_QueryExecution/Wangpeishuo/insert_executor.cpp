//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(child_executor_),
    ctlg_(GetExecutorContext()->GetCatalog()) {}

void InsertExecutor::Init()
{
    if (!plan_->IsRawInsert())  //如果插入的值不是来自二维向量
    {
        child_executor_->Init(); //字执行器需要init一下
    }
    else
    {
        idx = 0;
    }
}

//返回false说明没有tuple了。insert操作不需要返回这俩参数！
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool
{
    if (plan_->IsRawInsert())//如果插入的值来自二维向量
    {
        if (idx == plan_->RawValues().size() - 1) //向量内的值没了
            return false;
        *tuple = Tuple(plan_->RawValuesAt(idx), &ctlg_->GetTable(plan_->TableOid())->schema_);
        ++idx;
        //如果执行到这了，说明tuple已经是我要插入的tuple了
    }
    else//如果插入的值不是来自二维向量，而是来自子节点
    {
        if (!child_executor_->Next(tuple, rid)) //子节点的NEXT返回false说明没有了
            return false;
        //如果执行到这了，说明tuple已经是我要插入的tuple了
    }
    //下面直接插入就可以了，需要在TableHeap里操作
    ctlg_->GetTable(plan_->TableOid())->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction());
    //表插入完了，可是还要插入索引，下面这个是所有索引
    std::vector<IndexInfo*> index_ = ctlg_->GetTableIndexes(ctlg_->GetTable(plan_->TableOid())->name_);
    for (auto it : index_)
    {
        it->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());
    }
    return true;
}

}  // namespace bustub
