# 实现思路

所有任务状态分为NoAllocated,NoDone两个阶段用map队列进行存储，并通过它们长度进行判断目前处于哪个阶段。任务处理周期定为十秒，若十秒未处理完则认为超时但并不报错只是将其状态变回NoAllocated状态。


# 踩到的坑

- 按照教程运行会出现cannot load plugin wc.so报错，经网上查找将命令改为go build -buildmode=plugin -gcflags="all=-N -l"  ../mrapps/wc.go后报错小时
- 文件生成后命名格式不对导致reduce无法识别，需要调用rename函数重命名。
