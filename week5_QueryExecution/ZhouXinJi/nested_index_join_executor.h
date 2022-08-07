//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param outer table child
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  Tuple CombineTuples(Tuple *outer_tuple, Tuple *inner_tuple);

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  TableMetadata *inner_table_info_;
  IndexInfo *inner_index_info_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  // outer表的每一条tuple，在inner中都可以通过index一次扫描出与之匹配的tuple的RID，并存放在list中
  std::vector<RID> inner_matched_tuple_rid_list_;
  Tuple outer_tuple_;
};
}  // namespace bustub
