**实现思路:**  
worker与coordinator的交互：这里设计成worker通过heartbeat向coordinator索要task  
coordinator 设计两个阶段（MapPhase，ReducePhase），以及当前阶段的task pool（mapPhases和reducePhases，均设置为bool型数组）  
当有worker 来索要 task 时，从 task pool 中取出一个 task 给该 worker，同时， coordinator 启动异步线程检查这个 task 是否完成（go关键字实现），这里设定的超时时间为10s，如果在这个时间内没有完成，则将该task放回，返回false   
worker的实现参考了mrsequential.go，通过heartbeat获得task时则开始执行，执行完后发送rpc告知coordinator，并索要下一个task。如果当前没有task的话（WorkerWait阶段），就sleep一会再继续尝试  

**测试：**  
初步通过test-mr.sh的测试点，但是会出现DATA RACE，目前还没有搞明白  
