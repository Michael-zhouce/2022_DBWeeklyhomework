#include <map>
#include <string>
#include <list>
#include <functional> 
#include <algorithm>

#ifndef __CONSISTENT_HASH_H__
#define __CONSISTENT_HASH_H__

//consistent hash的节点类型。
//一元函数对象。接收T类型对象作为参数，返回一个整形作为其hash值，该hash值将被用于内部的排序。Hash需在其内部定义result_type 指明返回整形的类型。
//template<class T>和template<typename T> 都可以用来定义函数模板和类模板，在使用上没有本质的区别。
//调用这样的模板函数时，类型T就会被调用时的类型所代替，程序中的值就不再受限于类型。
template <typename T,typename Hash,typename Alloc = std::allocator<std::pair<const typename Hash::result_type, T > > >

class consistent_hash_map
{
public:
    //typedef创建了存在类型的别名，而typename告诉编译器map_type::iterator是一个类型而不是一个成员。
    typedef typename Hash::result_type size_type;
    //使用std::map来管理节点
    //map对象是模板类，需要关键字和存储对象两个模板参数，例如：
    //std: map<int, CString> enumMap;这样就定义了一个用int作为关键字检索CString条目的map对象
    //Alloc==less<type>()  从小到大排序， grater<type>()  从大到小排序 
    typedef std::map<size_type, T, std::less<size_type>, Alloc> map_type;
    typedef typename map_type::value_type value_type;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    //map<int, int>::iterator it; 定义一个迭代指针it。 it->first 为索引键值，it->second 为值。
    typedef typename map_type::iterator iterator;//正向迭代器，类似指针
    typedef typename map_type::reverse_iterator reverse_iterator;//反向迭代器
    typedef Alloc allocator_type;

    consistent_hash_map() {}
    ~consistent_hash_map() {}

    std::size_t size() const {
        return nodes_.size();
    }

    bool empty() const {//判断consistent_hash_map是否为空
        return nodes_.empty();
    }
    //插入一个节点，如果返回值中bool变量为真，iterator则为指向插入节点的迭代器。如果bool为假，表示插入失败，iterator指向已经存在的节点。
    //插入失败因为节点已经存在或者是节点的hash值与其他节点发生冲突。
    //pair是将2个数据组合成一个数据，当需要这样的需求时就可以使用pair，如stl中的map就是将key和value放在一起来保存
    std::pair<iterator, bool> insert(const T& node) {
        size_type hash = hasher_(node);
        return nodes_.insert(value_type(hash, node));
    }

    iterator find(size_type hash) {//通过传入的hash值找对其在consistent_hash中对应的节点的迭代器
        if (nodes_.empty()) {
            return nodes_.end();
        }

        iterator it = nodes_.lower_bound(hash);

        if (it == nodes_.end()) {
            it = nodes_.begin();
        }

        return it;
    }

    iterator begin() { return nodes_.begin(); }//返回对应迭代器
    iterator end() { return nodes_.end(); }
    reverse_iterator rbegin() { return nodes_.rbegin(); }//返回对应的反向迭代器
    reverse_iterator rend() { return nodes_.rend(); }


private:

    Hash hasher_;
    map_type nodes_;
};


#endif