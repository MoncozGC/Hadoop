package com.JadePenG.spark.step04

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * spark Streaming整合kafka0.10版本
  *
  * 使用之前在Pom中打开kafka0.10的依赖
  *
  * @author Peng
  */
object KafkaDirectStreamV10 {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    //批处理时间为5秒钟
    val ssc = new StreamingContext(conf, Seconds(5))

    /**
      * kafka0.10版本配置
      * 从哪里开始消费数据[latest, earliest, none]
      * earliest：当各个分区下有已递交的offset时，从递交的offset开始消费，无递交的offset时，从头开始消费
      * latest：当各个分区下有已递交的offset时，从递交的offset开始消费，无递交的offset时，消费新产生的该分区下的数据
      * none：当个分区都存在已递交的offset时，从递交的offset开始消费，只要有一个分区不存在已递交的offset，则抛出异常
      */
    val kafkaParams = Map(
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "day062801",
      "auto.offset.reset" -> "earliest",
      "auto.commit.interval.ms" -> "1000", //设置1秒钟自动递交一次偏移量：主动递交偏移量有个缺点：两次递交之间会造成数据丢失
      "enable.auto.commit" -> (false: java.lang.Boolean) //是否自动递交偏移量
    )
    //指定主题
    val topics = Array("0702")

    /**
      * LocationStrategy: 位置策略
      * 包含了两个参数PreferBrokers、PreferConsistent
      * PreferBrokers：如果kafka的broker节点跟spark的executor节点在同一台服务器的话，就使用它
      * PreferConsistent：如果kafka的broker节点跟spark的executor节点不在同一台服务器的话，就使用它
      * 在企业中多数情况下，kafka的broker和spark的worker节点不会再一台服务器的
      * 设置位置策略的原因是：会以最优的策略进行读取数据
      * 如果两者在同一台服务器的话，读写性能非常高，不需要网络传输
      * PreferConsistent：将从kafka拉取的数据尽量的平均分配到所有的executor节点
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    println("==========================================")

    //遍历rdd
    stream.foreachRDD(rdd => {
      //转换成 HasOffsetRanges
      val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRange) {
        //消费的主题  消费的分区  从哪里开始消费  消费到哪个位置
        //可以找到最新的消费位置, 那么我们自己就可以来维护偏移量了
        println(s"topic=${o.topic}, partition=${o.partition}, fromOffset=${o.fromOffset}, endOffset=${o.untilOffset}")
      }

      val reducedRDD: RDD[(String, Int)] = rdd.map(record => (record.value(), 1)).reduceByKey(_ + _)
      reducedRDD.foreachPartition(partition => {
        //初始化数据库连接
        partition.foreach(it => {
          //一行行将数据写入到mysql
        })
      })

      //主动发起递交偏移量  异步递交
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
    })
    //stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
