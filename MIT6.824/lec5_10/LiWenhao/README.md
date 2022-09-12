# 实现思路

lab2A 只需要按照figure2一步步定义向下写即可


# 踩到的坑

lab2A可能存在data race还未发现，在执行go test -run 2A可以通过，而go test -run 2A -race会报race
