package com.JadePenG.spark.step04

/**
  * 0.8版本 kafka消费者  Receiver
  * 使用之前将Pom文件中kafka0.8版本的依赖打开
  *
  * SparkStreaming整合kafka，使用createStream的方式消费数据
  * 我们的kafka有第三个partition，最好启动三个线程，一个线程消费一个分区的数据
  * 使用这种方式进行消费，我们的offset都会维护在zk上面
  *
  * 注意：
  * kafka中的topic的分区和SparkStreaming中生成的rdd的分区没有啥关系，
  * 在kafkaUtils.createStream中增加分区数量只会增加单个Receiver的线程数，不会增加spark的并行度
  * 可以创建多个kafka的输入DStream, 使用不同的group和topic, 使用多个receiver并行接收数据
  *
  * @author Peng
  */
object KafkaCreateStreamCP {

}
