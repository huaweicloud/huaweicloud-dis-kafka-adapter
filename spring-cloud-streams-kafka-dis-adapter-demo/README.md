## DEMO使用说明：
* 1、进入libs目录，执行sh 1_install_mvn_3rd_jars.sh
* 2、导入MAVEN工程
* 3、修改DisAdapterUtils类中DIS的接入参数
* 4、在DIS服务中创建application.yaml中定义的流名称，样例为greetings
* 5、运行StreamKafkaApplication，即可看到发送和接收数据

## 项目修改：
* 1、增加DISKafkaProducerFactory和DISKafkaConsumerFactory
* 2、修改了spring KafkaMessageChannelBinder的源代码，把KafkaProducerFactory和KafkaConsumerFactory替换为DISKafkaProducerFactory和DISKafkaConsumerFactory
* 3、将开源的spring-cloud-stream-binder-kafka-1.3.1.RELEASE.jar包中KafkaMessageChannelBinder类删除，新包即为spring-cloud-stream-binder-kafka-1.3.1.RELEASE-dis.jar
