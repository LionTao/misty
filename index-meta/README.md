# 索引元数据服务

> type: service module

存储索引元数据的service

## 功能

充当index的服务器发现service

index的一个actor启动后先来这注册，这里维护者一个树状结构，agent查询的时候会把这个树状结构拉到本地并进行搜索查询

## 难点

1. 树状结构高效搜索与向下延伸，基于h3
2. 树状结构要json可序列化
3. 使用类似rtree overlapping进行搜索——service本机只是保存一个列表，agent根据列表进行rtree创建
