package com.JadePenG.spark.step04

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  *0.10版本消费者
  *
  * @author Peng
  */
object KafkaDirectStreamV10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092, node02:9092, node01:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      //是否主动提交偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //指定主题
    val topics = Array("wordcount")

    /**
      * LocationStrategy: 位置策略
      * 包含了两个参数PreferBrokers、PreferConsistent
      * PreferBrokers：如果kafka的broker节点跟spark的exector节点在同一台服务器的话，就使用它
      * PreferConsistent：如果kafka的broker节点跟spark的exector节点不在同一台服务器的话，就使用它
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
      val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRange) {
        println(s"topic=${o.topic}, partition=${o.partition}, fromOffset=${o.fromOffset}, endOffset=${o.untilOffset}")
      }

      val reducedRDD: RDD[(String, Int)] = rdd.map(record => (record.value(), 1)).reduceByKey(_ + _)
      reducedRDD.foreachPartition(partition => {
        //初始化数据库连接
        partition.foreach(it => {
          //一行行将数据写入到mysql
        })
      })

      //主动发起递交偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
    })
    //stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
