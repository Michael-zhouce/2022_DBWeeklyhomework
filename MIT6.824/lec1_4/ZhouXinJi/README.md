# 实现思路

所有的任务都是由worker主动索求，而coordinator被动地分配任务给worker。coordinator记录当前是MapPhase、ReducePhase或者FinishPhase，不同的阶段coordinator给worker不同的任务。所有的任务获取和状态更新都是通过RPC实现

![](https://s1.ax1x.com/2022/08/22/vyHEm8.png)

# 踩到的坑

- RPC函数必须带有返回值error且必须仅有两个参数，否则再调用RPC的时候就会提示找不到这个函数
- RPC传递的参数首字母必须大写
- defer c.mutex.Lock()，应该是Unlock的，可恶啊，debug了一个小时硬是没有发现问题出在哪里，当发现的时候能够气个半死。