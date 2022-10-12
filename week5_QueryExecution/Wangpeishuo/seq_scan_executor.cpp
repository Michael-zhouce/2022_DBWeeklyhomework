//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
	: AbstractExecutor(exec_ctx), //��ʼ���Ӹ���̳�����context����
	plan_(plan),
	table_schema_(&(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)),
	//heap_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()) //û����Ӧ���캯�����õ�ʱ��������
	txn(exec_ctx_->GetTransaction()),
	iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->Begin(txn))
	{}

void SeqScanExecutor::Init()
{
	//����iterָ��
	iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
	//�����Ȳ��Ƿ�Ϊĩβ
	if (iter_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->End())
	{//û��
		return false;
	}
	*tuple = *iter_; //�õ�������tuple
	*rid = tuple->GetRid(); //����RID
	//�����ȿ���ν�ʺ���ͶӰ
	//plan_->GetPredicate()�����е�ν��
	if (plan_->GetPredicate() != nullptr)
	{
		if (!plan_->GetPredicate()->Evaluate(tuple, table_schema_).GetAs<bool>())
		{
			return Next(tuple, rid);
		}
	}
	//�������plan���schema����ͶӰ
	int num = plan_->OutputSchema()->GetColumnCount();
	std::vector<Value> values;
	for (int i = 0; i < num; i++)
	{
		values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, plan_->OutputSchema()));
	}
	*tuple = Tuple(values, plan_->OutputSchema()); //ֻ����ͶӰ���schema
	
	return true;
}

}  // namespace bustub
