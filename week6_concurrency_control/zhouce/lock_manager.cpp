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
#include "concurrency/transaction_manager.h"

#include <queue>
#include <utility>
#include <vector>

namespace bustub {
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when unlock");

 
  auto following_it = lock_request_queue.request_queue_.erase(it);

  
  if (following_it != lock_request_queue.request_queue_.end() && !following_it->granted_ &&
      LockManager::IsLockCompatible(lock_request_queue, *following_it)) {
    lock_request_queue.cv_.notify_all();
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  return true;
}
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // LOG_DEBUG("try to lock shared on rid: %d, %d by txn: %d", rid.GetPageId(), rid.GetSlotNum(),
  // txn->GetTransactionId());
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortImplicitly(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::SHARED);

  // wait
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  lock_request.granted_ = true;

  if (!txn->IsExclusiveLocked(rid)) {
    txn->GetSharedLockSet()->emplace(rid);
  }

  return true;
}
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::EXCLUSIVE);

  // wait
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  lock_request.granted_ = true;

  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
 
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];
  l.unlock();

  std::unique_lock<std::mutex> queue_latch(lock_request_queue.latch_);
  if (lock_request_queue.upgrading_) {
    AbortImplicitly(txn, AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_request_queue.upgrading_ = true;
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when upgrade lock");
  BUSTUB_ASSERT(it->granted_, "Lock request has not be granted");
  BUSTUB_ASSERT(it->lock_mode_ == LockManager::LockMode::SHARED, "Lock request is not locked in SHARED mode");

  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "Rid is not shared locked by transaction when upgrade");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Rid is currently exclusive locked by transaction when upgrade");

  it->lock_mode_ = LockManager::LockMode::EXCLUSIVE;
  it->granted_ = false;
  // wait
  lock_request_queue.cv_.wait(queue_latch, [&lock_request_queue, &lock_request = *it, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  it->granted_ = true;
  lock_request_queue.upgrading_ = false;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}
}  // namespace bustub
