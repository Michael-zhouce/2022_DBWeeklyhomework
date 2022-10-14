//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "abstract_executor.h"
#include "distinct_plan.h"
#include "hash_util.h"

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the distinct */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
     //根据一个tuple，得到distinct里面要求的所有的关键字的值组成的一个向量。
     DistinctKey MakeDistinctKey(const Tuple* tuple) {
         std::vector<Value> keys;
         for (uint64_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
             keys.emplace_back(tuple->GetValue(plan_->OutputSchema(), i));
         }
         return { keys };
     }
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  SimpleDistinctHashTable dht_;
  SimpleDistinctHashTable::Iterator dht_iterator_;
};
}  // namespace bustub

//需要模仿agg里面实现的散列方法---------------------------------------------------
namespace bustub {
struct DistinctKey {
    std::vector<Value> values;

    auto operator==(const DistinctKey& other) const -> bool {
        for (uint32_t i = 0; i < other.values.size(); i++) {
            if (values[i].CompareEquals(other.values[i]) != CmpBool::CmpTrue) {
                return false;
            }
        }
        return true;
    }
};
}  // namespace bustub

namespace std {

    /** Implements std::hash on DistinctKey */
    template <>
    struct hash<bustub::DistinctKey> {
        auto operator()(const bustub::DistinctKey& agg_key) const -> std::size_t {
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
    class SimpleDistinctHashTable {
    public:
        void Insert(const DistinctKey& distinct_key, const Tuple& tuple)
        {
            if(ht_.count(distinct_key) == 0)
                ht_.insert({ distinct_key, tuple });
        }

        class Iterator {
        public:
            /** Creates an iterator for the aggregate map. */
            explicit Iterator(std::unordered_map<DistinctKey, Tuple>::const_iterator iter) : iter_{ iter } {}

            /** @return The value of the iterator */
            const Tuple& Val() { return iter_->second; }

            Iterator& operator++() {
                ++iter_;
                return *this;
            }

            /** @return `true` if both iterators are identical */
            bool operator==(const Iterator& other) { return this->iter_ == other.iter_; }
        private:
            /** Aggregates map */
            std::unordered_map<DistinctKey, Tuple>::const_iterator iter_;
        };
        Iterator Begin() { return Iterator{ ht_.cbegin() }; }
        Iterator End() { return Iterator{ ht_.cend() }; }

    private:
        //这玩意是从distinct的关键字到tuple的映射，因为是distinct，所以就一个tuple
        std::unordered_map<DistinctKey, Tuple> ht_{};
    };
}