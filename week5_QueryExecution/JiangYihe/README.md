**Sequential Scan**：顺序扫描，通过tableheap的Next方法获取数据，通过select字段来生成tuple  
**Insert**：插入，调用tableheap的InsertTuple进行插入，成功插入数据后也将索引插入  
            在实现insert_executor.cpp时，对InsertExecutor的Next方法在各种情况下的返回值要仔细判断，捋清逻辑，稍不注意就会出现heap-buffer-overflow，也是折腾了很久  
**Update**：更新，通过给出的GenerateUpdatedTuple生成新的tuple，然后调用tableheap的UpdateTuple进行更新。由于无法原地更新索引，所以对索引采取了先删除再插入新的方法  
**Delete**：删除，调用tableheap的MarkDelete进行删除，然后删除索引  
**Nested Loop Join**：调用predicate的EvaluateJoin方法，返回符合条件的左右tuple  
**Hash Join**：使用unorder_map实现简单hash table（仿照SimpleAggregationHashTable），在初始化的时候插入所有的左孩子tuple，之后调用Next逐个匹配右孩子tuple  
**Aggregation**：头文件已经实现SimpleAggregationHashTable，调用EvaluateAggregate判断Having条件，符合返回，不符合则继续查找  
**Limit**：维护计数变量numbers_，通过GetLimit实现  
**Distinct**：类似Hash Join，通过hash table去重  
  
**测试**：本地初步通过executor_test的所有测试点，未进行gradescope评分测试  
**总结**：实现这个项目之前一定要仔细阅读相关代码，如相关的plan文件，executors文件等，捋清执行的逻辑和各个类之间的关系，不然很容易懵  
