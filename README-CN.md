# flink-sql-connector-redis

## 介绍

基于 [bahir-flink](https://github.com/apache/bahir-flink) [yangyichao-mango](https://github.com/yangyichao-mango/flink-study)

复用 connector：复用 bahir 提供的 redis connnector

复用 format：复用 flink 目前的 format 机制

简洁性：实现 kv 结构。hget 封装一部分

维表 local cache：为避免高频率访问 redis，维表加了 local cache 作为缓存