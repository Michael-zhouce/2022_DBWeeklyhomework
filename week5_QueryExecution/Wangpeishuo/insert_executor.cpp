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
    if (!plan_->IsRawInsert())  //��������ֵ�������Զ�ά����
    {
        child_executor_->Init(); //��ִ������Ҫinitһ��
    }
    else
    {
        idx = 0;
    }
}

//����false˵��û��tuple�ˡ�insert��������Ҫ��������������
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool
{
    if (plan_->IsRawInsert())//��������ֵ���Զ�ά����
    {
        if (idx == plan_->RawValues().size() - 1) //�����ڵ�ֵû��
            return false;
        *tuple = Tuple(plan_->RawValuesAt(idx), &ctlg_->GetTable(plan_->TableOid())->schema_);
        ++idx;
        //���ִ�е����ˣ�˵��tuple�Ѿ�����Ҫ�����tuple��
    }
    else//��������ֵ�������Զ�ά���������������ӽڵ�
    {
        if (!child_executor_->Next(tuple, rid)) //�ӽڵ��NEXT����false˵��û����
            return false;
        //���ִ�е����ˣ�˵��tuple�Ѿ�����Ҫ�����tuple��
    }
    //����ֱ�Ӳ���Ϳ����ˣ���Ҫ��TableHeap�����
    ctlg_->GetTable(plan_->TableOid())->table_->InsertTuple(*tuple, rid, GetExecutorContext()->GetTransaction());
    //��������ˣ����ǻ�Ҫ���������������������������
    std::vector<IndexInfo*> index_ = ctlg_->GetTableIndexes(ctlg_->GetTable(plan_->TableOid())->name_);
    for (auto it : index_)
    {
        it->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());
    }
    return true;
}

}  // namespace bustub
