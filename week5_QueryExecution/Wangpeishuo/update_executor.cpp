//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())),
    child_executor_(std::move(child_executor)), //what?
    tbhp_(table_info_->table_.get())
    {}

void UpdateExecutor::Init()
{
    child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool
{
    //�ȴ���ԭ����
    Tuple pretuple = *tuple;
    RID prerid = *rid;
    if (!child_executor_->Next(tuple, rid)) //���û��tuple��
    {
        return false;
    }
    //����Ѿ����е����ˣ�tuple�Ѿ�����Ҫ�ĵ�tuple��
    *tuple = GenerateUpdatedTuple(*tuple);
    //��tableheap���
    tbhp_->UpdateTuple(*tuple, *rid, GetExecutorContext()->GetTransaction());
    //���������
    std::vector<IndexInfo*> index_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->name_);
    for (auto it : index_)
    {
        it->index_->DeleteEntry(pretuple, prerid, GetExecutorContext()->GetTransaction());//ɾ�ɵ�
        it->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());//�����µ�
    }
    return true;
}

//�������еĴ��룬�����Ǹ�һ��tuple��ֱ�������µ�tuple�����ء�
auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
