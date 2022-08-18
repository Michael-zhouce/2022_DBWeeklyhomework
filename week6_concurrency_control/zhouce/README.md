1.LockUpgrade(Transaction, RID)   将写锁变为排他锁，可以和读锁共存
2.只需要修改两个文件concurrency/lock_manager.cpp和concurrency/lock_manager.h
transaction.h 里有一些事物的描述，如果有事物对当前rid加了共享锁。则必须等共享锁释放之后才能再加X锁。如果有事物对当前rid加了X锁之后，则在该X锁释放之前，不会有任何的锁被施加
