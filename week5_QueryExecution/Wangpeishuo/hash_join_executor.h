//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "executor_context.h"
#include "abstract_executor.h"
#include "hash_join_plan.h"
#include "tuple.h"
#include "hash_util.h"
#include "abstract_expression.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:

     //根据一个tuple，得到哈希连表里面要求的所有的关键字的值组成的一个向量。
     HashKey MakeHashKey(const Tuple* tuple, int x)
     { //x = 1说明是左孩子表的tuple，否则是右边的孩子表tuple
         std::vector<Value> keys;
         if (x == 1)
             keys.push_back(plan_->LeftJoinKeyExpression()->Evaluate(tuple, left_child_->GetOutputSchema()));
         else
             keys.push_back(plan_->RightJoinKeyExpression()->Evaluate(tuple, right_child_->GetOutputSchema()));
         return { keys };
     }

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  //构造函数中给了孩子，这里也加上
  const std::unique_ptr<AbstractExecutor> left_child_;
  const std::unique_ptr<AbstractExecutor> right_child_;
  //模仿agg给出的
  SimpleHashJoinHashTable jht_{};
  //让实现变的简单一点
  std::queue < Tuple > res{};
};

}  // namespace bustub

//需要模仿agg里面实现的散列的方法--------------------------------------------------------------
namespace bustub {
    struct HashKey //这个类型的对象是一堆value的集合，代表一种元组
    {
        std::vector<Value> values;

        auto operator==(const HashKey& other) const -> bool {
            for (uint32_t i = 0; i < other.values.size(); i++) {
                if (values[i].CompareEquals(other.values[i]) != CmpBool::CmpTrue) {
                    return false;
                }
            }
            return true;
        }
    };

    struct HashValue  //这个类型的对象代表一个桶，装的是相同的元组
    {
        std::vector<Tuple> same_tuples;
    };
}  // namespace bustub

namespace std {

    /** Implements std::hash on DistinctKey */
    template <>
    struct hash<bustub::HashKey> {
        auto operator()(const bustub::HashKey& agg_key) const -> std::size_t {
            size_t curr_hash = 0;
            for (const auto& key : agg_key.values) {
                if (!key.IsNull()) {
                    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
                }
            }
            return curr_hash;
        }
    };

}  // namespace std

namespace bustub {
    class SimpleHashJoinHashTable {
    public:
        //Init函数里就应该给所有左孩子tuple使用insert函数。
        void Insert(const HashKey& hash_key, const Tuple& tuple)
        {
            if (ht_.count(hash_key) == 0)
            {
                HashValue hv;
                hv.same_tuples.push_back(tuple);
                ht_.insert({ hash_key, hv});
            }
            else
            {
                ht_[hash_key].same_tuples.push_back(tuple);
            }
        }
        //数一下有没有这个key！
        uint64_t Count(const HashKey& hash_key)
        {
            return ht_.count(hash_key);
        }
        //得到列表
        HashValue GetTuples(const HashKey& hash_key)
        {
            return ht_[hash_key];
        }

    private:
        std::unordered_map<HashKey, HashValue> ht_{};
    };
}  // namespace bustub