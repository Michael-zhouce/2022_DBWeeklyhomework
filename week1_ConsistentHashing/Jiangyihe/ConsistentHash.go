package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashRing sort的接口
type HashRing []uint32

func (x HashRing) Len() int {
	return len(x)
}

func (x HashRing) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x HashRing) Less(i, j int) bool {
	return x[i] < x[j]
}

type Consistent struct { //一致性结构体
	circle           map[uint32]string //哈希环，存储node(string)和虚拟node(uint32)的对应关系
	nodes            map[string]bool   //node类型为string，bool返回该node是否存在
	NumOfReplication int               //node对应的虚拟node的个数，初始值为20
	sortedHashes     HashRing          //已经排好序的hash slice，储存全部hash值
	count            int               //物理node
	sync.RWMutex                       //并发控制锁
}

func New() *Consistent { //初始化
	c := new(Consistent)
	c.circle = make(map[uint32]string)
	c.nodes = make(map[string]bool)
	c.NumOfReplication = 20
	return c
}

func (c *Consistent) HashKey(key string) uint32 { //哈希算法
	return crc32.ChecksumIEEE([]byte(key)) //使用IEEE多项式返回数据的crc32校验和
}

func (c *Consistent) ElementKey(element string, index int) string { //生成虚拟node的key
	return strconv.Itoa(index) + element //返回字符型，得到key值
}

func (c *Consistent) UpdateSortedHashes() { //更新已排序
	hashes := c.sortedHashes[0:0] //从0开始到0结束（一个环）
	for k := range c.circle {
		hashes = append(hashes, k) //append添加索引
	} //通过对c.circle这个map进行range，返回索引值
	sort.Sort(hashes) //通过Sort进行排序更新,以便于二分查找
	c.sortedHashes = hashes
}

func (c *Consistent) Nodes() []string { //获取全部node
	c.Lock() //上锁
	defer c.Unlock()
	var Nods []string
	for k := range c.nodes {
		Nods = append(Nods, k) //通过返回索引获取node
	}
	return Nods //返回包含全部实体node的string数组
}

func (c *Consistent) Add(element string) {
	c.Lock() //上锁
	defer c.Unlock()
	for i := 0; i < c.NumOfReplication; i++ {
		c.circle[c.HashKey(c.ElementKey(element, i))] = element
	} //根据element和i生成的key来生成哈希值，并把哈希值放在环上
	c.nodes[element] = true //nodes返回bool型true，添加成功
	c.UpdateSortedHashes()  //更新
	c.count++               //数目+1
}

func (c *Consistent) search(key uint32) int {
	//sort.Search二分查找c.sortedHashes长度大于参数key的最小值，顺时针第一个
	i := sort.Search(len(c.sortedHashes), func(x int) bool { return c.sortedHashes[x] > key })
	if i >= len(c.sortedHashes) {
		i = 0
	} //超出范围则将i置0
	return i
}

func (c *Consistent) Get(key string) string { //获取search的结点
	c.Lock()
	defer c.Unlock()

	i := c.search(c.HashKey(key))
	return c.circle[c.sortedHashes[i]]
}

type Server struct { //真实结点结构体
	Name  string
	Store []int //通过store数组存放数据验证一致性哈希是否成功
}

func printStore(c *Consistent, num int, servers *map[string]*Server) {
	for k := range *servers {
		(*servers)[k].Store = (*servers)[k].Store[0:0] //从0到0，避免重复数据
	}
	for i := 0; i < num; i++ {
		server := c.Get(fmt.Sprintf("%d", i))                          //格式化输出，调用Get函数
		(*servers)[server].Store = append((*servers)[server].Store, i) //append添加索引
	}
	for k, v := range *servers {
		fmt.Println(k, " store:", v.Store) //向Store数组添加元素
	}
}

func main() {
	c := New()
	servers := make(map[string]*Server) //make方法初始化
	//添加7个真实结点
	servers["node A"] = &Server{"node A", make([]int, 0)}
	servers["node B"] = &Server{"node B", make([]int, 0)}
	servers["node C"] = &Server{"node C", make([]int, 0)}
	servers["node D"] = &Server{"node D", make([]int, 0)}
	servers["node E"] = &Server{"node E", make([]int, 0)}
	servers["node F"] = &Server{"node F", make([]int, 0)}
	servers["node G"] = &Server{"node G", make([]int, 0)}
	for _, server := range servers {
		c.Add(server.Name) //给servers中的元素添加名字
	}

	fmt.Println("初始状态数据分布")
	printStore(c, 80, &servers)

	//看看key13在哪个node
	fmt.Println(c.Get("13"))

	servers["node H"] = &Server{"node H", make([]int, 0)}
	c.Add("node H")
	fmt.Println("\n在加入node H后的数据状态分布")
	printStore(c, 80, &servers)

	//查询当前hash环的具体信息
	fmt.Println("members size:", len(c.nodes), "\ncircle:", len(c.circle), "\nsortHashes:", len(c.sortedHashes))

	//再看看key13到了哪（查询key值获取对应真实node）
	fmt.Println(c.Get("13"))
	//for k, v := range c.sortedHashes {
	//	fmt.Println(k, c.circle[v])
	//}
}

/*
由运行结果可看出，在添加 node H 后，仅对哈希环上相邻node产生影响（node B，node C）
通过Get查询得到当前key映射的node，key可作为索引更新
*/
