//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())),
    tbhp_(table_info_->table_.get()),
    child_executor_(std::move(child_executor))
{}

void DeleteExecutor::Init()
{
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool
{
    if (!child_executor_->Next(tuple, rid)) //���û��tuple��
    {
        return false;
    }
    //����Ѿ����е����ˣ�tuple�Ѿ�����Ҫ�ĵ�tuple��
    //��tableheap����ɾ��(ֻ�Ǳ����)
    tbhp_->MarkDelete(*rid, GetExecutorContext()->GetTransaction());
    //��������ɾ��
    std::vector<IndexInfo*> index_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->name_);
    for (auto it : index_)
    {
        it->index_->DeleteEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());
    }
    return true;

}

}  // namespace bustub
