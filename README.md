# Zero-Flink
Zero-Flink
# 1. 概述
## 1.1. 简介
Flink是一个批处理和流处理结合的统一计算框架，其核心是一个提供了数据分发以及并行化计算的流数据处理引擎。它的最大亮点是流处理，
是业界最顶级的开源流处理引擎。Flink最适合的应用场景是低时延的数据处理（Data Processing）场景：高并发pipeline处理数据，时延毫秒级，且兼具可靠性。

# 2. 功能

# 3. 使用
## 3.1. 环境搭建
### 3.1.1. 安装Flink
- [https://nightlies.apache.org/flink/flink-docs-master/zh/docs/try-flink/local_installation/](https://nightlies.apache.org/flink/flink-docs-master/zh/docs/try-flink/local_installation/)
### 3.1.2. 安装Netcat
> Netcat（又称为NC）是一个计算机网络工具，它可以在两台计算机之间建立 TCP/IP 或 UDP 连接。它被广泛用于测试网络中的端口，发送文件等操作。
> 使用 Netcat 可以轻松地进行网络调试和探测，也可以进行加密连接和远程管理等高级网络操作。因为其功能强大而又简单易用，所以在计算机安全领域也有着广泛的应用。****
```shell
# 下载
yum install -y nc
# 测试
nc -lk 8888
```
# 4. 其他

# 5. 参考