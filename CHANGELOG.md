# 1.1.1

- Features
  * add kafka clients 1.1.0 supports
  * add kafka clients 0.11 supports
  
# 1.1.2
- Features
  * add dis spring kafka demo

# 1.1.3
- Features
  * add common adapter

# 1.2.3
- Features
  * use huaweicloud-sdk-dis-java 1.3.4 version

# 1.2.7
- Features
  * Add periodic heartbeat configuration, and use periodic heartbeat by default
  
# 1.2.11
- Bugs
  * 修复关闭消费者时定时心跳线程和异步提交 Offset 线程无法关闭的问题
  
# 1.2.13
- Bugs
  * 修复 Rebalance 时直接 rejoin 的消费者没有从服务端同步 Checkpoint 的问题
  
# 1.2.14
- Features
  * add kafka clients 2.1.1 supports
  
# 1.2.15
- Features
  * 支持通过配置`rebalance.timeout.ms`参数来配置加入消费组的超时时间
  
# 1.2.16
- Bugs
  * 修复部分 Rebalance 场景下定时心跳线程占用 CPU 过高的问题

# 1.2.18
- Bugfixs
  * 下载数据接口，当app不为null时，添加app参数

# 1.2.19
- Bugfixs
  * 支持跨账号授权

# 1.2.20
- Bugfixs
  * jackson-databind 类加载死锁问题处理：https://github.com/FasterXML/jackson-databind/issues/2715