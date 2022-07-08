// consistentHash project main.go
package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type virtualNode struct {
	name        string          //如:serverA#1,serverA#2
	virtualHash uint32          //虚拟节点map到ring上的哈希值
}

type node struct {
	name          string        //如:serverA,serverB
	ip            string        //如:192.168.1.1
	virturalNodes []virtualNode //结点包含的虚拟节点集合
}

type obj struct {
	name string
}

type consistentHash struct {
	replicaNum  int             //虚拟节点数
	virtualList []uint32        //虚拟节点map到ring的有序哈希值集合
	virtualMap  map[uint32]node //虚拟节点哈希值对应的真实节点
	lock        sync.RWMutex    //读写锁
}

var initNodes = []node{ //初始节点
	{"serverA", "192.168.1.1", nil},
	{"serverB", "192.72.1.2", nil},
	{"serverC", "192.1.255.255", nil},
	{"serverD", "192.1.1.123", nil},
}

var initObjs = []obj{
	{"objectA"},
	{"objectB"},
	{"objectC"},
	{"objectD"},
}

func hashFunc(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func binarySearch(key uint32, list []uint32) (int, uint32, bool) {
	var left, mid, right int = 0, 0, len(list) - 1
	for left < right {
		mid = (left + right) / 2
		if key > list[mid] {
			left = mid + 1
		} else if key < list[mid] {
			right = mid - 1
		} else {
			return mid, key, true
		}
	}
	return mid, 0, false
}

func initFunc(h *consistentHash) bool {

	h.replicaNum = 10
	h.virtualList = make([]uint32, 20)
	h.virtualMap = make(map[uint32]node)
	for _, xNode := range initNodes {
		h.addNode(xNode)
	}
	return true
}

func (h *consistentHash) getNode(x obj) node { //获取节点
	h.lock.RLock()                               //上读锁
	defer h.lock.RUnlock()
	xHash := hashFunc(x.name)                    //xHash:对象x映射到ring上的哈希值
	mid, key, tag := binarySearch(xHash, h.virtualList)
	if tag == false { //对象哈希值在两个节点哈希值之间
		key = h.virtualList[mid]
		if xHash > h.virtualList[mid] {
			key = h.virtualList[mid+1]
		}
	}
	fmt.Printf("对象%s(Hash值:%d)在节点%s上\n", x.name, xHash, h.virtualMap[key].name)
	return h.virtualMap[key]
}

func (h *consistentHash) addNode(newNode node) bool { //新增节点
	h.lock.Lock()                                       //上互斥锁
	defer h.lock.Unlock()
	for i := 0; i < h.replicaNum; i++ {
		vName := newNode.name + "#" + strconv.Itoa(i)
		vHash := hashFunc(newNode.ip + strconv.Itoa(i))
		v := virtualNode{vName, vHash}                    //创建虚拟节点
		newNode.virturalNodes = append(newNode.virturalNodes, v)
		h.virtualList = append(h.virtualList, vHash)
		h.virtualMap[vHash] = newNode
	}
	sort.Slice(h.virtualList, func(i, j int) bool {     //节点哈希值按顺序排列
		return h.virtualList[i] < h.virtualList[j]
	})
	return true
}

func (h *consistentHash) deleteNode(delNode node) bool { //删除节点
	h.lock.Lock()                                          //上互斥锁
	defer h.lock.Unlock()
	for _, v := range delNode.virturalNodes {
		delete(h.virtualMap, v.virtualHash)                  //删除相应的map
		pos, _, _ := binarySearch(v.virtualHash, h.virtualList)
		h.virtualList = append(h.virtualList[:pos], h.virtualList[pos+1:]...)
		//通过将pos前与pos后的切片合并实现pos位置哈希值的删除
	}
	delNode.virturalNodes = nil                            //清空切片
	return true
}

func main() {
	var h consistentHash
	initFunc(&h)
	for _, x := range initObjs {
		h.getNode(x)
	}
	var node1 node
	node1.name = "serverE"
	node1.ip = "127.0.0.1"
	node1.virturalNodes = nil
	h.addNode(node1)
	var obj1 obj
	obj1.name = "objectE"
	node2 := h.getNode(obj1)
	h.deleteNode(node2)
	h.getNode(obj1)
}
