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
    //先存上原来的
    Tuple pretuple = *tuple;
    RID prerid = *rid;
    if (!child_executor_->Next(tuple, rid)) //如果没有tuple了
    {
        return false;
    }
    //如果已经运行到这了，tuple已经是我要改的tuple了
    *tuple = GenerateUpdatedTuple(*tuple);
    //在tableheap里改
    tbhp_->UpdateTuple(*tuple, *rid, GetExecutorContext()->GetTransaction());
    //在索引里改
    std::vector<IndexInfo*> index_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->name_);
    for (auto it : index_)
    {
        it->index_->DeleteEntry(pretuple, prerid, GetExecutorContext()->GetTransaction());//删旧的
        it->index_->InsertEntry(*tuple, *rid, GetExecutorContext()->GetTransaction());//插入新的
    }
    return true;
}

//这是已有的代码，作用是给一个tuple，直接生成新的tuple并返回。
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
