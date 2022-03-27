# flink-sql-connector-redis

[中文](README-CN.md)

## Introduction

base on [bahir-flink](https://github.com/apache/bahir-flink) [yangyichao-mango](https://github.com/yangyichao-mango/flink-study)

Multiplexing connector: multiplexing the redis connector provided by bahir

Reuse format: Reuse flink's current format mechanism

Conciseness: Implement the kv structure. hget package part

Dimension table local cache: To avoid frequent access to redis, dimension table adds local cache as a cache