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
	: AbstractExecutor(exec_ctx), //初始化从父类继承来的context变量
	plan_(plan),
	table_schema_(&(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)),
	//heap_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()) //没有相应构造函数，用的时候来复制
	txn(exec_ctx_->GetTransaction()),
	iter_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->Begin(txn))
	{}

void SeqScanExecutor::Init()
{
	//调整iter指针
	iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
	//必须先查是否为末尾
	if (iter_ == exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->End())
	{//没了
		return false;
	}
	*tuple = *iter_; //得到完整的tuple
	*rid = tuple->GetRid(); //返回RID
	//下面先考虑谓词后考虑投影---------------------------------------------------------
	// LockManager在虚基类里的ExecutorContext里
	// project4加入：上锁
	LockManager* lm = GetExecutorContext()->GetLockManager();
	Transaction* txn = GetExecutorContext()->GetTransaction();
	//如果是READ_UNCOMMITTED，就什么都不做，不然就上s锁
	if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED)
	{
		lm->LockShared(txn, *rid);
	}
	//plan_->GetPredicate()是所有的谓词
	if (plan_->GetPredicate() != nullptr)
	{
		if (!plan_->GetPredicate()->Evaluate(tuple, table_schema_).GetAs<bool>())
		{
			return Next(tuple, rid);
		}
	}

	//下面根据plan里的schema进行投影
	int num = plan_->OutputSchema()->GetColumnCount();
	std::vector<Value> values;
	for (int i = 0; i < num; i++)
	{
		values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, plan_->OutputSchema()));
	}
	*tuple = Tuple(values, plan_->OutputSchema()); //只返回投影后的schema
	
	// project4加入：解锁，只有READ_COMMITTED才在这个时候急着解锁，Repeatableread不管，等最后解锁
	if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)
	{
		lm->Unlock(txn, *rid);
	}

	return true;
}

}  // namespace bustub
