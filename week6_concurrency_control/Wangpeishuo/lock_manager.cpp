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

#include "lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool
{
	if(txn->GetState() == TransactionState::ABORTED)
		return false;
	else if (txn->GetState() == TransactionState::SHRINKING)
	{
		txn->SetState(TransactionState::ABORTED);
		throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
		return false;
	}
	//Read Uncommitted不用s锁
	if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) 
	{
		txn->SetState(TransactionState::ABORTED);
		throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
		return false;
	}
	//考虑以前有了的情况
	if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) 
	{
		return true;
	}

	std::unique_lock<std::mutex> lk(latch_);

	LockRequest lock_request(txn->GetTransactionId(), LockMode::SHARED);
	lock_table_[rid].request_queue_.emplace_back(lock_request);
	
	while (waits(txn->GetTransactionId(), &lock_table_[rid]))
	{
		lock_table_[rid].cv_.wait(lk);
	}
	
	//记录一下，同意了
	for (auto it : lock_table_[rid].request_queue_)
	{
		if (it.txn_id_ == txn->GetTransactionId())
		{
			it.granted_ = true;
		}

	}
	txn->GetSharedLockSet()->emplace(rid); //获取锁
	return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool
{
	if (txn->GetState() == TransactionState::ABORTED)
		return false;
	else if (txn->GetState() == TransactionState::SHRINKING)
	{
		txn->SetState(TransactionState::ABORTED);
		throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
		return false;
	}
	//考虑以前有了的情况
	if (txn->IsExclusiveLocked(rid))
	{
		return true;
	}
	std::unique_lock<std::mutex> lk(latch_);

	LockRequest lock_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
	lock_table_[rid].request_queue_.emplace_back(lock_request);

	while (waitx(txn->GetTransactionId(), &lock_table_[rid]))
	{
		lock_table_[rid].cv_.wait(lk);
	}

	//记录一下，同意了
	for (auto it : lock_table_[rid].request_queue_)
	{
		if (it.txn_id_ == txn->GetTransactionId())
		{
			it.granted_ = true;
		}

	}
	txn->GetExclusiveLockSet()->emplace(rid); //获取锁
	return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool
{
	if (txn->GetState() == TransactionState::ABORTED)
		return false;
	else if (txn->GetState() == TransactionState::SHRINKING)
	{
		txn->SetState(TransactionState::ABORTED);
		throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
		return false;
	}
	if (!txn->IsSharedLocked(rid)) //无s锁
	{
		return false;
	}
	//考虑以前有了的情况
	if (txn->IsExclusiveLocked(rid))
	{
		return true;
	}
	std::unique_lock<std::mutex> lk(latch_);

	while (waitu(txn->GetTransactionId(), &lock_table_[rid]))
	{
		lock_table_[rid].cv_.wait(lk);
	}
	for (auto it : lock_table_[rid].request_queue_)
	{
		if (it.txn_id_ == txn->GetTransactionId())
		{
			it.granted_ = true;
			it.lock_mode_ = LockMode::EXCLUSIVE;
			txn->GetSharedLockSet()->erase(rid);
			txn->GetExclusiveLockSet()->emplace(rid);
			break;
		}
	}
	return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool
{
	if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid))
	{
		return false;
	}
	std::unique_lock<std::mutex> lk(latch_);
	//把这个id删掉
	if (lock_table_[rid].upgrading_ == txn->GetTransactionId())
	{
		lock_table_[rid].upgrading_ = INVALID_TXN_ID;
	}
	else return false;

	for (auto it = lock_table_[rid].request_queue_.begin(); it != lock_table_[rid].request_queue_.end(); it++)
	{
		if (it->txn_id_ == txn->GetTransactionId())
		{
			lock_table_[rid].request_queue_.erase(it);
			lock_table_[rid].cv_.notify_all(); //唤醒所有等待的线程
			break;
		}
	}
	if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)
	{
		txn->SetState(TransactionState::SHRINKING);
	}
	txn->GetSharedLockSet()->erase(rid);
	txn->GetExclusiveLockSet()->erase(rid);
	return true;
}

bool LockManager::waits(txn_id_t id, LockRequestQueue* lock_queue)
{
	for (auto& request : lock_queue->request_queue_)
	{
		if (request.txn_id_ == id)
		{
			return true;
		}
		if (request.lock_mode_ == LockMode::EXCLUSIVE)
		{
			return false;
		}
	}
	return true;
}

bool LockManager::waitx(txn_id_t id, LockRequestQueue* lock_queue)
{
	auto it = lock_queue->request_queue_.begin();
	if (it->txn_id_ != id) return false;
	return true;
}

bool LockManager::waitu(txn_id_t id, LockRequestQueue* lock_queue)
{
	auto it = lock_queue->request_queue_.begin();
	if (it->txn_id_ != id) return false;
	return true;
}

}  // namespace bustub
