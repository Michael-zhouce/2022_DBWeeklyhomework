//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
  using B_PLUS_TREE_ITERATOR_TYPE = IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
  using B_PLUS_TREE_INDEX_TYPE = BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;

 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  Tuple GenerateTuple(const Tuple &tuple, const Schema &output_schema);

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  TableHeap *table_heap_ptr = nullptr;
  Transaction *txn_ = nullptr;
  B_PLUS_TREE_ITERATOR_TYPE iterator_;
  B_PLUS_TREE_ITERATOR_TYPE iter_end_;
};
}  // namespace bustub
