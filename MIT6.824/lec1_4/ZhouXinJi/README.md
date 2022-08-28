# 实现思路

所有的任务都是由worker主动索求，而coordinator被动地分配任务给worker。coordinator记录当前是MapPhase、ReducePhase或者FinishPhase，不同的阶段coordinator给worker不同的任务。所有的任务获取和状态更新都是通过RPC实现

![](https://s1.ax1x.com/2022/08/22/vyHEm8.png)

# 踩到的坑

- RPC函数必须带有返回值error且必须仅有两个参数，否则再调用RPC的时候就会提示找不到这个函数
- RPC传递的参数首字母必须大写
- defer c.mutex.Lock()，应该是Unlock的，可恶啊，debug了一个小时硬是没有发现问题出在哪里，当发现的时候能够气个半死。
- encoding/json.NewEncoder()传递的参数一定要用os.Create()打开，不要用os.OpenFile打开，要不然不知道为什么会多好多条数据出来。
- 在map任务结束之前，一定要用tmp file防止reduce任务读取到非完整数据，否则会出大问题。
- 当worker向coordinator索要任务的时候，如果当前没有未开始的任务并且当前阶段的任务还没有全部完成的时候（任务正在运行），需要让worker等待一段时间。

