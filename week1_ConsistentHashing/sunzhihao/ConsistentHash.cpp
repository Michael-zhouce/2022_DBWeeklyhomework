#include <iostream>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <stdio.h>
using namespace std;

class ConsistentHash {
public:
    set<string> realnode;  // 真实节点 
    int vnodecount;  // 每个真实节点的虚拟节点个数
    map<int, string> serverNodes;  //每个虚拟节点的ip和对应的哈希值
    ConsistentHash(int vnodecount) 
    {
        this->vnodecount = vnodecount;
    };
    ~ConsistentHash() {
        serverNodes.clear();
    };
    int Hash(string key);  
    void Initialize();
    void AddNewRealNode(const string& nodeIp);
    void DeleteRealNode(const string& nodeIp);
    string GetServerIndex(const string& key);
    void StatisticPerf(std::string& label, int left, int right);
};

//哈希函数（32 位的 Fowler-Noll-Vo 哈希算法）
int ConsistentHash::Hash(string key) {
    const int p = 16777619;
    int hash = 2166136261;
    for (int idx = 0; idx < key.size(); ++idx) {
        hash = (hash ^ key[idx]) * p;
    }
    hash += hash << 13;
    hash ^= hash >> 7;
    hash += hash << 3;
    hash ^= hash >> 17;
    hash += hash << 5;
    if (hash < 0) {
        hash = 0 - hash;
    }
    return hash;
}

/*
该函数为初始化函数，主要用来初始化map serverNodes，首先加入3个真实节点ip，
而后通过给定的虚拟节点个数，利用字符串拼接得到虚拟结点的ip，
再对虚拟节点做Hash，并最终得到serverNodes，完成一致性哈希的节点初始化
*/
void ConsistentHash::Initialize()
{
    realnode.insert(string("192.168.100.1"));
    realnode.insert(string("192.168.100.2"));
    realnode.insert(string("192.168.100.3"));
    for (auto& ip : realnode) {
        for (int j = 0; j < vnodecount; ++j) {
            stringstream nodeKey;
            nodeKey << ip << "#" << j;
            int partition = Hash(nodeKey.str());
            serverNodes.insert({ partition, ip });
        }
    }
}
// 添加一个真实节点
void ConsistentHash::AddNewRealNode(const string& nodeIp)
{
    for (int j = 0; j < vnodecount; ++j) {
        stringstream nodeKey;
        nodeKey << nodeIp << "#" << j;
        int partition = Hash(nodeKey.str());
        serverNodes.insert({ partition, nodeIp });
    }
}

//删除一个真实节点
void ConsistentHash::DeleteRealNode(const string& nodeIp)
{
    for (int j = 0; j < vnodecount; ++j) {
        stringstream nodeKey;
        nodeKey << nodeIp << "#" << j;
        int partition = Hash(nodeKey.str());
        auto it = serverNodes.find(partition);
        if (it != serverNodes.end()) {
            serverNodes.erase(it);
        }
    }
}

//查询数据所在节点
string ConsistentHash::GetServerIndex(const string& key)
{
    int partition = Hash(key);
    auto it = serverNodes.lower_bound(partition);
    // 沿环的顺时针找到一个大于等于 partition 的虚拟节点
    if (it == serverNodes.end()) {
        if (serverNodes.empty()) {
            cout << "no available nodes" << endl;
        }
        return serverNodes.begin()->second;
    }
    return it->second;
}

// 将数据存储在合适的节点上，并输出每个节点所存储的数据比例
void ConsistentHash::StatisticPerf(string& label, int objMin, int objMax) {
    map<string, int> cnt;
    for (int i = objMin; i <= objMax; i++) {
        string nodeIp = GetServerIndex(to_string(i));
        cnt[nodeIp]++;
    }
    int total = objMax - objMin + 1;
    cout << "==== " << label << " ====" << endl;
    for (auto& p : cnt) {
        cout << "nodeIp: " << p.first << " rate: " << p.second * 100 / (total * 1.0) << "%" << endl;
    }
}

int main() {
    //初始化无虚拟节点的一致性哈希
    ConsistentHash ch(1);
    ch.Initialize();
    string label = "无虚拟节点";
    ch.StatisticPerf(label, 0, 65536);

    //初始化有虚拟节点的一致性哈希

    ConsistentHash ch2(32);
    ch2.Initialize();
    string label2 = "有虚拟节点";
    ch2.StatisticPerf(label2, 0, 65536);
    //添加一个节点
    ch2.AddNewRealNode("192.168.100.4");
    string label3 = "添加一个节点";
    ch2.StatisticPerf(label3, 0, 65536);

    //删除一个节点
    ch2.DeleteRealNode("192.168.100.1"); 
    string label4 = "删除一个节点";
    ch2.StatisticPerf(label4, 0, 65536);

    //查询哈希值为2345的数据所在的节点
    cout<<"查询功能"<<endl;
    cout <<"2345所在的节点为："<< ch2.GetServerIndex(to_string(2345)) << endl;
    return 0;
}
