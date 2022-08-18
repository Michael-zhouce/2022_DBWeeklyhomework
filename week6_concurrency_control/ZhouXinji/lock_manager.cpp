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

#include <queue>
#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::CheckBeforeLock(Transaction *txn) {
  bool result = false;
  // In 2PL, it's not allowed to accquire lock on shrinking phase.
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("transaction %d abort beacause of lock_on_shrinking", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return result;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return result;
  }
  result = true;
  return result;
}

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(Transaction *txn, const RID &rid) {
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  auto iter = lock_request_queue->request_queue_.begin();
  while (iter != lock_request_queue->request_queue_.end()) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      break;
    }
    iter++;
  }
  return iter;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn)) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("transaction %d abort because of lockshared_on_read_uncommitted", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  std::unique_lock<std::mutex> lock(latch_);
  {
    auto iter = GetIterator(txn, rid);
    if (iter != lock_table_[rid].request_queue_.end() && iter->lock_mode_ == LockMode::SHARED) {
      return true;
    }
  }

  txn->SetState(TransactionState::GROWING);

  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  lock_request_queue->request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  if (lock_request_queue->is_writting) {
    lock_request_queue->cv_.wait(lock, [lock_request_queue, txn]() {
      return !lock_request_queue->is_writting || txn->GetState() == TransactionState::ABORTED;
    });
  }

  // Aborted because of deadlock
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(txn, rid);
    lock_request_queue->request_queue_.erase(iter);
    LOG_DEBUG("transaction %d abort because of deadlock", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  lock_request_queue->reader_num += 1;
  auto iter = GetIterator(txn, rid);
  iter->granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn)) {
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  {
    auto iter = GetIterator(txn, rid);
    if (iter != lock_table_[rid].request_queue_.end() && iter->lock_mode_ == LockMode::EXCLUSIVE) {
      return true;
    }
  }
  txn->SetState(TransactionState::GROWING);

  LockRequestQueue *lock_request_queue = &lock_table_[rid];

  // If conflicts, blocking
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (lock_request_queue->is_writting || lock_request_queue->reader_num != 0) {
    std::cout << "write blocking on:" << rid << "transaction id is:" << txn->GetTransactionId() << "\n";

    lock_request_queue->cv_.wait(lock, [lock_request_queue, txn]() {
      return (!lock_request_queue->is_writting && lock_request_queue->reader_num == 0) ||
             (txn->GetState() == TransactionState::ABORTED);
    });
  }

  // Aborted because of deadlock
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(txn, rid);
    lock_request_queue->request_queue_.erase(iter);
    LOG_DEBUG("transaction %d abort because of deadlock", txn->GetTransactionId());
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  lock_request_queue->is_writting = true;
  auto iter = GetIterator(txn, rid);
  iter->granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (!CheckBeforeLock(txn) || !IsLockShared(txn, rid)) {
    return false;
  }
  std::unique_lock<std::mutex> lock(latch_);
  LockRequestQueue *lock_request_queue = &lock_table_[rid];
  auto iter = GetIterator(txn, rid);
  lock_request_queue->request_queue_.erase(iter);
  lock_request_queue->request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request_queue->reader_num -= 1;
  if (lock_request_queue->reader_num > 0) {
    lock_request_queue->cv_.wait(lock, [lock_request_queue, txn]() {
      return (!lock_request_queue->is_writting&& lock_request_queue->reader_num == 0) ||
             txn->GetState() == TransactionState::ABORTED;
    });
  }
  lock_request_queue->is_writting = true;

  {
    auto iter = GetIterator(txn, rid);
    iter->granted_ = true;
  }

  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (txn->GetSharedLockSet()->count(rid) != 0) {
    std::unique_lock<std::mutex> lock(latch_);

    // Remove lock_request from lock_table
    LockRequestQueue *lock_request_queue = &lock_table_[rid];
    auto iter = GetIterator(txn, rid);
    if (iter != lock_request_queue->request_queue_.end()) {
      lock_request_queue->request_queue_.erase(iter);
      lock_request_queue->reader_num -= 1;
    }

    // Wake up blocking thread
    lock_request_queue->cv_.notify_one();
    txn->GetSharedLockSet()->erase(rid);
    return true;
  }

  if (txn->GetExclusiveLockSet()->count(rid) != 0) {
    std::unique_lock<std::mutex> lock(latch_);

    // Remove lock_request from lock_table
    LockRequestQueue *lock_request_queue = &lock_table_[rid];
    auto iter = GetIterator(txn, rid);
    if (iter != lock_request_queue->request_queue_.end()) {
      lock_request_queue->request_queue_.erase(iter);
      lock_request_queue->is_writting = false;
    }

    // Wake up blocking thread
    lock_request_queue->cv_.notify_all();
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

void LockManager::RemoveNode(txn_id_t id) {
  // Remove edges that arc tail is id.
  waits_for_.erase(id);

  // Remove edges that arc head is id. They are waiting for the resources that transaction "id" owes.
  Transaction *txn = TransactionManager::GetTransaction(id);
  for (auto const &shared_rid : *txn->GetSharedLockSet().get()) {
    for (auto const &lock_request : lock_table_[shared_rid].request_queue_) {
      if (!lock_request.granted_) {
        RemoveEdge(lock_request.txn_id_, id);
      }
    }
  }
  for (auto const &exclusive_rid : *txn->GetExclusiveLockSet().get()) {
    for (auto const &lock_request : lock_table_[exclusive_rid].request_queue_) {
      if (!lock_request.granted_) {
        RemoveEdge(lock_request.txn_id_, id);
      }
    }
  }
}

bool LockManager::DFS(txn_id_t txn_id) {
  bool has_cycle = false;
  if (visited[txn_id] == 1) {
    auto iter = std::find(trace_.begin(), trace_.end(), txn_id);
    // This node has been searching, which means this grapha has a circle.
    if (iter != trace_.end()) {
      has_cycle = true;
      txn_id_t max_txn_id = -1;
      LOG_DEBUG("has circle!");
      while (iter != trace_.end()) {
        if (*iter > max_txn_id) {
          max_txn_id = *iter;
        }
        std::cout << *iter << " ";
        iter++;
      }
      if (max_txn_id < newest_txn_id_) {
        newest_txn_id_ = max_txn_id;
      }
      std::cout << "\n";
      return has_cycle;
    }
    has_cycle = false;
    return has_cycle;  // Searched, not searching.
  }
  visited[txn_id] = 1;  // visited
  trace_.emplace_back(txn_id);
  for (txn_id_t neighbor : waits_for_[txn_id]) {
    has_cycle |= DFS(neighbor);
  }
  trace_.pop_back();
  return has_cycle;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  visited.clear();
  newest_txn_id_ = 0x0fffffff;
  bool has_cycle = false;
  for (auto const &pair : waits_for_) {
    has_cycle |= DFS(pair.first);
  }
  *txn_id = newest_txn_id_;
  return has_cycle;
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
      std::unordered_map<txn_id_t, std::vector<RID>> to_request_resources;
      // Step1: build the dependency graph
      // waits_for_.clear();
      for (auto const &item : lock_table_) {
        std::list<LockRequest> granted_request_list;
        std::list<LockRequest> blocking_request_list;

        // Step1.1: Find the blocking_request_list and granted_request_list.
        for (auto const &lock_request : item.second.request_queue_) {
          if (lock_request.granted_) {
            granted_request_list.emplace_back(lock_request);
          } else {
            to_request_resources[lock_request.txn_id_].emplace_back(item.first);
            blocking_request_list.emplace_back(lock_request);
          }
        }

        // Step1.2: Add edges into graph.
        for (auto const &block_request : blocking_request_list) {
          for (auto const &granted_request : granted_request_list) {
            AddEdge(block_request.txn_id_, granted_request.txn_id_);
          }
        }
      }

      // Step2: Check if the graph has a circle and break circle if exists.
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        // LOG_DEBUG("has circle");
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        RemoveNode(txn_id);
        for (RID const &rid : to_request_resources[txn_id]) {
          lock_table_[rid].cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
