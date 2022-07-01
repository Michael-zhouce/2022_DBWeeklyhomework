/*
 * The implementation of consistent hashing.
 *
 * Usually, consistent hashing serve two purposes：
 *   * distrubed cache server
 *   * distributed databases
 * The latter is more complicated which involves data replication. 
 * Inorder to simplify the question to better understand consistent hashing, 
 * I decied to implement the first one.
 * References:
 *   * https://ably.com/blog/implementing-efficient-consistent-hashing
 *   * https://www.toptal.com/big-data/consistent-hashing
 *
 * @author Xinji Zhou
 * @date 2022.6.24
 */
#include "consistent_hashing.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <list>
#include <vector>
#include <algorithm>
#include <string.h>
#include "xxhash.h"

GlobalController *gc = nullptr;
DataBase *db = nullptr;

/**
 * @brief Verify whether the IP address is correct
 * 
 * @param ip IP address
 */
bool IsValidIP(std::string ip) {
    std::string patter = "(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])" \
                         ":*([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5])";
    std::regex ipv4(patter);
    return std::regex_match(ip, ipv4);
}

/**
 * @brief Construct a new Global Controller:: Global Controller object
 * 
 * @param filename Absolute path to server information file
 */
GlobalController::GlobalController(const char* filename) : server_num_(0) {
    std::ifstream in(filename);
    if (in.eof()) {
        std::cout << "cannot open " << filename << "\n";
    }
    std::string tmp;
    while (!in.eof()) {
        std::getline(in, tmp);
        if (tmp[0] == '#') {
            continue;  // Skip comment
        }
        char ip[30];  // IP address, e.g. 172.32.19.12:12333
        int num_of_virtual_nodes;  // Num of virtual nodes
        std::istringstream record(tmp);
        record >> ip;
        record >> num_of_virtual_nodes;
        addServer(ip, num_of_virtual_nodes);
    }

}

bool GlobalController::addServer(char* ip, int num_of_virtual_nodes) {
    std::lock_guard<std::mutex> locker(mutex_);
    if (!IsValidIP(ip)) {
        std::cout << "Invalid IP address:" << ip << "\n";
    }
    Server *new_server = new Server(ip);
    servers_[server_num_++] = new_server;
    Node node;
    node.server_ = servers_[server_num_-1];
    for (int i = 0; i < num_of_virtual_nodes; ++i) {
        char ss[32];
        sprintf(ss, "%s-%d", ip, i);
        node.point_ = XXH32(ss, strlen(ss), 0);
        continuum_.push_back(node);

    }
    std::sort(continuum_.begin(), continuum_.end());
    // printf("Test address:%p\n", continuum_.back().server_);
}

/**
 * @brief User API. Client uses key to get value. Then, GloballController will
 * select the corresponding server to get value and return it to client.
 * 
 * @param key 
 * @return ValueType 
 */
ValueType GlobalController::Get(KeyType key) {
    std::lock_guard<std::mutex> locker(mutex_);
    Server *server = GetServer(key);
    if (server == nullptr) {
        std::cout << "ERROR:No server available!\n";
        return "";
    }
    return server->Get(key);
}

bool GlobalController::Put(KeyType key, ValueType value) {
    std::lock_guard<std::mutex> locker(mutex_);
    Server *server = GetServer(key);
    if (server == nullptr) {
        std::cout << "ERROR:No server available!\n";
        return false;
    }
    return server->Put(key, value);
}

ValueType DataBase::Get(KeyType key) {
    if (data_.find(key) == data_.end()) {
        return "";
    }
    return data_[key];
}

bool DataBase::Put(KeyType key, ValueType value) {
    data_[key] = value;
    return true;
}

/**
 * @brief Get the server to which the key belongs.
 * I use binary-search to find the correct server poniter;
 * 
 * @param key Key in key-value pairs
 * @return Server* The server poniter
 */
Server* GlobalController::GetServer(KeyType key) {
    std::lock_guard<std::mutex> locker(mutex_);
    if (continuum_.size() == 0) {
        return nullptr;
    }
    size_t lowp = 0;
    size_t highp = continuum_.size();
    size_t middlep;
    uint32_t h = XXH32(key.data(), key.size(), 0);
    uint32_t midval, midval1;
    Server *ret = nullptr;
    while (1) {
        middlep = (lowp + highp) / 2;
        if (middlep == continuum_.size()) {
            ret = continuum_[0].server_;  // if it's at the end, roll back to zeroth
            break;
        }
        midval = continuum_[middlep].point_;
        midval1 = middlep == 0 ? 0 : continuum_[middlep-1].point_;
        if (h <= midval && h > midval1) {  // find the corresponding position
            ret = continuum_[middlep].server_;
            break;
        }
        if (h > midval) {
            lowp = middlep + 1;  // server should be on the right;
        } else {
            highp = middlep - 1;  // server should be on the left;
        }

        if (lowp > highp) {
            ret = continuum_[0].server_;
            break;
        }
    }
    // std::cout << *ret;
    return ret;
}

/**
 * @brief Delete server by IP address. Corresponding the server breakdown scenarios
 * 
 * @param ip 
 * @return true 
 * @return false 
 */
bool GlobalController::DelServer(const char *ip) {
    std::lock_guard<std::mutex> locker(mutex_);
    int pos = -1;
    for (int i = 0; i < server_num_; ++i) {
        if (servers_[i]->GetIP() == ip) {
            pos = i;
            break;
        }
    }
    if (pos == -1) {
        std::cout << "Server doesn't exists!\n";
        return false;
    }
    for (auto it = continuum_.begin(); it != continuum_.end();) {
        if (it->server_->GetIP() == ip) {
            it = continuum_.erase(it);
        } else {
            it++;
        }
    }
    delete servers_[pos];
    for (int i = pos; i < server_num_ - 1; ++i) {
        servers_[i] = servers_[i+1];
    }

}

Server::Server(std::string ip, int num_of_virtual_nodes) {
    ip_ = ip;
    num_of_virtual_nodes_ = num_of_virtual_nodes;
}

/**
 * @brief Get value from corresponding server
 * 
 * @param key Key in key-value pairs
 * @return ValueType Value corresponding to key
 */
ValueType Server::Get(KeyType key) {
    std::lock_guard<std::mutex> locker(mutex_);
    std::cout << "Getting value from server " << GetIP() << "\n";
    if (cache_.find(key) == cache_.end()) {
        ValueType value = db->Get(key);
        if (value == "") {
            return "";
        } else {
            cache_[key] = value;
            return value;
        }
    }
    return cache_[key];
}

/**
 * @brief Put key-value pairs to corresponding server
 * 
 * @param key key in key-value pairs
 * @param value value in key-value pairs
 * @return true if execute successfully
 * @return false if error occurs
 */
bool Server::Put(KeyType key, ValueType value) {
    std::lock_guard<std::mutex> locker(mutex_);
    std::cout << "Putting key-value pairs to server " << GetIP() << "\n";
    cache_[key] = value;
    db->Put(key, value);
    return true;
}

/**
 * @brief print server's information
 * 
 * @param os 
 * @param server 
 * @return std::ostream& 
 */
std::ostream& operator<<(std::ostream& os, const Server &server) {
    os << "server's IP address:" << server.ip_ << "\n";
    os << "server's weight:" << server.num_of_virtual_nodes_ << "\n";
    os << "key-value pairs:\n";
    for (auto &item : server.cache_) {
        os << item.first << " " << item.second;
    }
    return os;
}