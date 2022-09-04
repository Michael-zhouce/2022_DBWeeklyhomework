# Raft fault-tolerance

leader会向follower定期发送心跳包（通过空内容的RPC实现），当follower收到leader的心跳包后，就会重置自己的计时器。当follower在一段时间后没有收到leader的心跳时，计时器超时，此时follower就会变成candidate，并向其他节点要选票，当获得其他节点一半的选票后candidate就会变成leader；若在此期间收到了leader传来的心跳包，那么candidate就会重新变成follower。在我的设计中，心跳包单独使用一个goroutine来建立。

![raft](http://img.singhe.art/raft.png)

论文中其实给出了非常详细的设计细节，只要读懂了论文按照论文的步骤来实现即可。在本实验中最大的问题还是并发的各种错误，比如死锁，data race这些，在以后的并行程序设计中也需要按照一定的规范来完成，不然很容易出难以调试的bug。