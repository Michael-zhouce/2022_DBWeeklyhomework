#include<iostream>
#include<map>
#include<set>
#include <sstream>
using namespace std;
//保存服务器的ip地址和存的数据信息 
struct nodeinfo{
	string IP;
	map<uint32_t, std::string> data; //key是数据的hash值，value是数据本身的value 
	
};
class ConsistentHash {
private:
    map<uint32_t, nodeinfo> serverNodes;
        // 虚拟节点，key 是哈希值，value 是机器的 ip 地址
   // set<string> physicalNodes;  // 真实机器节点 ip
    //int virtualNodeNum;  // 每个机器节点关联的虚拟节点个数
public:
    ConsistentHash() {
//        physicalNodes.insert(std::string("192.168.1.101"));
//        physicalNodes.insert(std::string("192.168.1.102"));
//        physicalNodes.insert(std::string("192.168.1.103"));
//        physicalNodes.insert(std::string("192.168.1.104"));
    };
    ~ConsistentHash() {
        serverNodes.clear();
    };
    static uint32_t FNVHash(string key);
    void Initialize();
    void AddNewPhysicalNode(const string& nodeIp);
    void DeletePhysicalNode(const string& nodeIp);
     auto  GetServerIndex(uint32_t partition);
    void StatisticPerf(string& label, int left, int right);
    void ClientAddData(const string& key,const string& value);
    void ClientDelData(const string& key);
    void ClientDisplayData(const string& nodeIp);
    void ClientFindData(const string& key);
};
// 32 位的 Fowler-Noll-Vo 哈希算法
// https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function
uint32_t ConsistentHash::FNVHash(string key) {
    const int p = 16777619;
    uint32_t hash = 2166136261;
    for (int idx = 0; idx < key.size(); ++idx) {
        hash = (hash ^ key[idx]) * p;
    }
    hash += hash << 13;
    hash ^= hash >> 7;
    hash += hash << 3;
    hash ^= hash >> 17;
    hash += hash << 5;
    if (hash < 0) {
        hash = -hash;
    }
    return hash;
}
auto ConsistentHash::GetServerIndex(uint32_t partition)//数据寻找相应服务器 
{
    //uint32_t partition = FNVHash(key);
    auto it = serverNodes.lower_bound(partition);
    // 沿环的顺时针找到一个大于等于 partition 的虚拟节点
    if(it == serverNodes.end()) {
        if (serverNodes.empty()) {
            std::cout << "no available nodes" << '\n';
        }
        return serverNodes.begin();
    }
    return it;
}

void ConsistentHash::Initialize()//对node进行初始化 ,添加四个node节点 
{
	string ips[4]={"192.168.1.101","192.168.1.102","192.168.1.103","192.168.1.104"};
     cout << "==== " << "初始化 含有4个node" << " ====" << '\n';
    for(int i=0;i<4;i++) {
        
            
            uint32_t partition = FNVHash(ips[i]);
            nodeinfo nodeinfomation;
			nodeinfomation.IP=ips[i];
			cout<<"IP:"<<ips[i]<<"  "<<"HASH: "<<partition<<endl;
			serverNodes.insert({partition, nodeinfomation});
        }
    
}
// 插入一个物理节点时，调整已经存储的数据 
void ConsistentHash::AddNewPhysicalNode(const string& nodeIp)
{
		uint32_t partition = FNVHash(nodeIp);
	 auto it = GetServerIndex(partition);
        
        nodeinfo nodeinfomation;
		nodeinfomation.IP=nodeIp;	
        serverNodes.insert({partition, nodeinfomation});
        if (!it->second.data.empty()) {//将被删除节点的数据转移到下一个节点 
       	   for (auto i = it->second.data.begin(); i != it->second.data.end(); i++) {
       	   	auto serverNode = GetServerIndex(i->first);
	         serverNode->second.data.insert({i->first, i->second});//插入数据存储 
      
          	}
       }
        
    
}
void ConsistentHash::DeletePhysicalNode(const string& nodeIp)//删除物理节点 
{
    
        
        uint32_t partition = FNVHash(nodeIp);//通过ip查找相应映射的哈希值 
        auto it = serverNodes.find(partition);//找到服务器在环中的位置
        //?还有可能找不到！ 
        auto itnext=it; //找到下一个节点 
          	itnext++;
        if(itnext==serverNodes.end()) {//因为是个环 
        	itnext=serverNodes.begin();
		}
		
   
       if (!it->second.data.empty()) {//将被删除节点的数据转移到下一个节点 
       	   for (auto i = it->second.data.begin(); i != it->second.data.end(); i++) {
            	itnext->second.data.insert({i->first, i->second});//插入数据存储 
          	}
       }
       //	cout<<"nextshuju大小"<<itnext->second.data.size()<<endl; 
        if(it != serverNodes.end()) {
            serverNodes.erase(it);
        }
        
        
    
}
void ConsistentHash::ClientAddData(const string& key,const string& value){
	uint32_t partition = FNVHash(key);
	auto serverNode = GetServerIndex(partition);
	serverNode->second.data.insert({partition, value});//插入数据存储 
	cout<<"key:"<<key<<" "<<"HASH:"<<partition <<"存入："<<serverNode->second.IP<<endl;
	//cout<<"数据大小"<<nodeinfomation.data.size()<<endl;
}
void ConsistentHash::ClientDelData(const string& key){//通过key值删除数据 
	uint32_t partition = FNVHash(key);
	auto serverNode = GetServerIndex(partition);
	serverNode->second.data.erase(partition);//删除数据 
}
void ConsistentHash::ClientFindData(const string& key){//通过key值查找数据 
	uint32_t partition = FNVHash(key);
	auto serverNode = GetServerIndex(partition);
	auto it=serverNode->second.data.find(1);
	cout<<"数据存储节点IP: "<< serverNode->second.IP<<endl;
}
void ConsistentHash::ClientDisplayData(const string& nodeIp){// 展示节点数据 
	uint32_t partition = FNVHash(nodeIp);
	auto it = GetServerIndex(partition);
	 if (!it->second.data.empty()) {
       	   for (auto i = it->second.data.begin(); i != it->second.data.end(); i++) {
            	cout<<"HASH: "<<i->first<<"  "<<"value: "<<i->second<<endl;
          	}
       }
	
}

void client(ConsistentHash ch){
	ch.Initialize();
        while(1)//
        {
		printf ( "\n" );
		printf ( "\n" );
		printf ( "            	******************ConsistentHash*************\n" );
		printf ( "	______________________________________________________________________________\n" );
		printf ( "	|---->                       请选择输入你要操作的选择                    <----|\n" );
		printf ( "	|----> 1: 存入数据                                                         <----|\n" );
		printf ( "	|----> 2：删除数据                                                <----|\n" );
		printf ( "	|----> 3：删除节点                                                          <----|\n" );
		printf ( "	|----> 4：插入新的节点                                                          <----|\n" );
		printf ( "	|----> 5：查看节点数据                                              <----|\n" );
		printf ( "	|----> 6：查找数据                                             <----|\n" );
		printf ( "	|__________ ___________________________________________________________________|\n" );
		int choice;
		string s1,s2,s3,s4,s5;
		printf("请输入你的选择\n");
		scanf("%d",&choice);
		switch(choice)
		{
			 
			case 1:
				
			cin>>s1,cin>>s2;
			 	 ch.ClientAddData(s1,s2); 
			 
			break;
			case 2:
				cout<<"输入删除数据的key值"<<endl; 
				cin>>s1;
			 	 ch.ClientDelData(s1); 
			break;
			case 3:
				cout<<"输入删除节点的ip"<<endl; 
				cin>>s1;
			 	 ch.DeletePhysicalNode(s1);
			break;
			case 4:
				cout<<"输入插入节点的ip"<<endl; 
				cin>>s1;
			 	 ch.AddNewPhysicalNode(s1);
			break;
			case 5:
				cout<<"输入查看节点的ip"<<endl; 
				cin>>s1;
			 	 ch.ClientDisplayData(s1);
			break;
			case 6:
				cout<<"输入查看节点的key值"<<endl; 
				cin>>s1;
			 	 ch.ClientFindData(s1);
			break;
				
        }
        
        }

} 
int main(){
	
	ConsistentHash ch;
    client(ch);
    
	return 0;
	
}
