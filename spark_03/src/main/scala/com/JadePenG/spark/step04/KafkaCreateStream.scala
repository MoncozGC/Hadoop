//package com.JadePenG.spark.step04
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * 0.8版本 kafka消费者  Receiver
//  *使用之前将Pom文件中kafka0.8版本的依赖打开
//  *
//  * SparkStreaming整合kafka，使用createStream的方式消费数据
//  * 我们的kafka有第三个partition，最好启动三个线程，一个线程消费一个分区的数据
//  * 使用这种方式进行消费，我们的offset都会维护在zk上面
//  *
//  * 注意：
//  * kafka中的topic的分区和SparkStreaming中生成的rdd的分区没有啥关系，在kafkaUtils.createStream中增加分区数量只会增加单个reciver的线程数，不会增加spark的并行度
//  * 可以创建多个kafka的输入DStream, 使用不同的group和topic, 使用多个receiver并行接收数据
//  *
//  * @author Peng
//  */
//object KafkaCreateStream {
//  def main(args: Array[String]): Unit = {
//    //创建streamingContext
//    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val zk = "node01:2181,node02:2181,node03:2181"
//    val group = "01"
//    //指定topic主题，和主题分区数
//    val topic = Map("0701" -> 3)
//
//    /**
//      * 三个线程处理三个分区的数据，定义个集合，调用三次，产生三个线程，一起去消费kafka中的数据
//      * ReceiverInputDStream:就是封装了我们的kafka的数据
//      */
//    val receiveStream = (1 to 3).map(x => {
//      val createStream = KafkaUtils.createStream(ssc, zk, group, topic)
//      createStream
//    })
//
//    //将我们各个线程（三个）里面的rdd数据，合并成一个大的DStream，DStream就是封装了我们一段时间(5S)的kafka中的所有的数据
//    //(String, String): 第一个参数表示我们数据的key， 第二个参数表示我们真实的数据
//    val unionDStream: DStream[(String, String)] = ssc.union(receiveStream)
//    val mapDStream = unionDStream.map(x => x._2)
//
//    mapDStream.print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
