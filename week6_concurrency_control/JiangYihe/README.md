**LockShared：S锁（读锁）**  
如果之前已经获得过S锁，直接return true即可  
如果当前阶段不是2PL的上锁阶段，直接abort掉  
除去以上这些情况就可以正常上S锁了，将S锁添加到等待队列和事务的S锁集中，等待获取锁，成功后把锁的granted_设为true  
**LockExclusive：X锁（写锁）**  
和S锁基本类似  
**LockUpgrade：将S锁升级为X锁（U锁）**  
和S锁基本类似，但是如果已经有了对应RID的X锁，说明不用Upgrade，直接返回true，并且只有当更老的事务不含有X锁时才能upgrade  
**Unlock：解锁**  
如果当前事务隔离级别是可重复读（REPETABLE_READ），且处于2PL的加锁阶段，将 2PL阶段设置为解锁阶段  
