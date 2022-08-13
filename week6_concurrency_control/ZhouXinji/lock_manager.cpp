//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>
#include <queue>

namespace bustub {


bool LockManager::CheckBeforeLock(Transaction *txn) {
  // In 2PL, it's not allowed to accquire lock on shrinking phase.
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn)) {
    return false;
  }

  // It shouldn't accuquire sharedlock for read uncommitted isolation level
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  txn->SetState(TransactionState::GROWING);

  std::unique_lock<std::mutex> lock(latch_);
  if (lock_table_.count(rid) == 0) {
    txn->GetSharedLockSet()->emplace(rid);
  }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn)) {
    return false;
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn)) {
    return false;
  }
  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (txn->GetSharedLockSet()->count(rid) != 0) {
    txn->GetSharedLockSet()->erase(rid);
    return true;
  }

  if (txn->GetExclusiveLockSet()->count(rid) != 0) {
    txn->GetExclusiveLockSet()->erase(rid);
    return true;
  }
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].emplace_back(t2);
  newest_txn_id_ = t1;
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.count(t1) == 0) {
    return;
  }
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unordered_map<txn_id_t, unsigned int> indegree;
  std::queue<txn_id_t> zero_indegree_queue;
  int total_node_num = 0;

  // Step 1: compute indegree of graph.
  for (auto const &item : waits_for_) {
    for (txn_id_t const &arc_head : item.second) {
      // Push arc_tail into indegree queue.
      if (indegree.count(item.first) == 0) {
        indegree.insert({item.first, 0});
      }

      // Push arc_head into indegree queue.
      if (indegree.count(arc_head) == 0) {
        indegree.insert({arc_head, 1});
      } else {
        indegree[arc_head] += 1;
      }
    }
  }
  total_node_num = indegree.size();

  // Step 2: push nodes whose indegree is zero into queue.
  int processed_nodes = 0;
  for (auto const &item : indegree) {
    if (item.second == 0) {
      zero_indegree_queue.push(item.first);
    }
  }

  while (!zero_indegree_queue.empty()) {
    processed_nodes += 1;
    txn_id_t arc_tail = zero_indegree_queue.front();
    zero_indegree_queue.pop();
    // Change the in-degree after removing the node.
    for (txn_id_t const &arc_head : waits_for_[arc_tail]) {
      indegree[arc_head] -= 1;
      // If the indegree becomes 0, then we should push it into queue.
      if (indegree[arc_head] == 0) {
        zero_indegree_queue.push(arc_head);
      }
    }
  }

  bool result = false;
  if (processed_nodes != total_node_num) {
    *txn_id = newest_txn_id_;
    result = true;
  }
  return result;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto const &item : waits_for_) {
    txn_id_t arc_tail = item.first;
    for (txn_id_t const &arc_head : item.second) {
      result.emplace_back(std::make_pair(arc_tail, arc_head));
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

}  // namespace bustub
