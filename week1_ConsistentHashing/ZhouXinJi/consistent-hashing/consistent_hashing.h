#include <vector>
#include <list>
#include <string>
#include <unordered_map>
#include <stdint.h>
#include <unordered_map>
#include <regex>
#include <mutex>
using KeyType = std::string;
using ValueType = std::string;

class Server;
using Node = struct Node;
struct Node {
    uint32_t point_;
    Server* server_;
    Node() {
        point_ = 0;
        server_ = nullptr;
    }
    Node(int point, Server* server) {
        point_ = point;
        server_ = server;
    }
    Node(const Node& other) {
        point_ = other.point_;
        server_ = other.server_;
    }
	bool operator<(const Node& other) const 
	{
		return point_ < other.point_;
	}

};

/**
 * @brief Global controller. Every client will only interact with global controller.
 * 
 */
class GlobalController {
private:
    std::mutex mutex_;  // Semaphore for Multithreading
    size_t server_num_;
    Server* servers_[64];
    std::vector<Node> continuum_;
    Server* GetServer(KeyType key);
public:
    GlobalController();
    GlobalController(const char* filename);
    bool addServer(char *ip, int num_of_virtual_nodes = 40);
    bool DelServer(const char *ip);
    ValueType Get(KeyType key);
    bool Put(KeyType key, ValueType value);
};

/**
 * @brief Origin source. All key-value pairs will be stored in there.
 * 
 */
class DataBase {
private:
    std::unordered_map<KeyType, ValueType> data_;
public:
    ValueType Get(KeyType key);
    bool Put(KeyType key, ValueType value);
};

/**
 * @brief Cache server. Each server may have many nodes in continuum, which is called 'virtual nodes'
 * 
 */
class Server {
private:
    std::mutex mutex_;  // Semaphore for Multithreading
    std::string ip_;
    int num_of_virtual_nodes_;
    std::unordered_map<KeyType, ValueType> cache_;
public:
    Server(std::string ip, int num_of_virtual_nodes = 40);
    inline std::string& GetIP() { return ip_; }
    ValueType Get(KeyType key);
    bool Put(KeyType key, ValueType value);
    friend std::ostream& operator<<(std::ostream& os, const Server &server);
};