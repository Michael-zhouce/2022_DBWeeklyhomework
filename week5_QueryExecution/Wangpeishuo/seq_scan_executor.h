//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "executor_context.h"
#include "abstract_executor.h"
#include "seq_scan_plan.h"
#include "tuple.h"
#include "table_iterator.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
	
	//下面加入一些类的成员
	TableIterator iter_;      //模拟的迭代器
	//下面这些其实plan_里已经包含了，但是这样用起来不方便
	    //TableHeap heap_;   //在虚基类的exec_ctx_里的catalog里的map里的table_里，需要用的oid在plan_里
	Schema* table_schema_; //表格的schema！
	Transaction* txn;  //事务，在exec_ctx_里
};
}  // namespace bustub
