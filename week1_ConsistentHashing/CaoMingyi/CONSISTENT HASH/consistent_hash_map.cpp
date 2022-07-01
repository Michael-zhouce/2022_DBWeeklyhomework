

#include <stdint.h>
#include <iostream>
#include <string>
#include <boost/functional/hash.hpp>
#include <boost/format.hpp>
#include <boost/crc.hpp>

#include "consistent_hash_map.h"


//所有主机的列表
const char* nodes[] = {
    "192.168.1.100",
    "192.168.1.101",
    "192.168.1.102",
    "192.168.1.103",
    "192.168.1.104"
};
//虚拟节点
struct vnode_t {
    vnode_t() {}
    vnode_t(std::size_t n, std::size_t v) :node_id(n), vnode_id(v) {}

    std::string to_str() const {
        return boost::str(boost::format("%1%-%2%") % nodes[node_id] % vnode_id);
    }

    std::size_t node_id;//主机ID,主机在主机列表中的索引
    std::size_t vnode_id;//虚拟节点ID

};

//hasher,使用CRC32作为hash算法，注意需要定义result_type
struct crc32_hasher {
    uint32_t operator()(const vnode_t& node) {
        boost::crc_32_type ret;
        std::string vnode = node.to_str();
        ret.process_bytes(vnode.c_str(), vnode.size());
        return ret.checksum();
    }
    typedef uint32_t result_type;
};

int main() {
    //为每个主机生成100个虚拟节点，然后加入consistent_hash_map
    //map对象是模板类，需要关键字和存储对象两个模板参数，例如：
    //std: map<int, CString> enumMap;这样就定义了一个用int作为关键字检索CString条目的map对象
    typedef consistent_hash_map<vnode_t, crc32_hasher> consistent_hash_t;
    consistent_hash_t consistent_hash_;//虚拟节点

    for (std::size_t i = 0; i < 5; ++i) {
        for (std::size_t j = 0; j < 100; j++) {
            consistent_hash_.insert(vnode_t(i, j));
        }
    }
    //查找某个hash值对应的vnode和主机
    consistent_hash_t::iterator it;//虚拟节点
    it = consistent_hash_.find(257894146);//通过传入的hash值找对其在consistent_hash中对应的节点的迭代器
    //map<int, int>::iterator it; 定义一个迭代指针it。 it->first 为索引键值，it->second 为值。
    //it -> first是该节点的hash值，it -> second是该虚拟节点。
    std::cout << boost::format("node:%1%,vnode:%2%,hash:%3%") % nodes[it->second.node_id] % it->second.vnode_id % it->first << std::endl;

    //遍历consistent_hash中的所有的vnode,统计每个虚拟节点的key的数量和每个主机包含key的数量
    std::size_t sums[] = { 0,0,0,0,0 };//设主机包含的key数量。
    consistent_hash_t::iterator i = consistent_hash_.begin(); //第一个节点
    consistent_hash_t::reverse_iterator j = consistent_hash_.rbegin(); //最后一个节点
    std::size_t n;
    //计算所有节点包含的key的数量,并更新主机包括的key的数量。
    uint32_t priv = j->first;
    uint32_t cur;
    consistent_hash_t::iterator end = consistent_hash_.end();//返回迭代器末尾
    while (i != end) {
        cur = i->first;
        n = cur - priv;
        std::cout << boost::format("vnode:%1%,hash:%2%,contains:%3%") % i->second.to_str() % cur % n << std::endl;
        sums[i->second.node_id] += n;//更新主机包含的key数量。
        priv = cur;
        i++;
    }

    for (std::size_t i = 0; i < 5; ++i) {
        std::cout << boost::format("node:%1% contains:%2%") % nodes[i] % sums[i] << std::endl;
    }
    system("pause");
    return 0;
}