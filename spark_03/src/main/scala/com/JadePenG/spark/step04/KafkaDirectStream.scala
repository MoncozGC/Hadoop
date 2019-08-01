//package com.JadePenG.spark.step04
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * kafka消费者  Direct
//  * Direct的方式消费数据, kafka会将所有的数据的offset保存在broker下的一个topic中[ __consumer_offsets ]
//  *
//  * @author Peng
//  */
//object KafkaDirectStream {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    val kafkaParams = Map(
//      "metadata.broker.list" -> "node01:9092, node02:9092, node03:9092",
//      "group.id" -> "0702"
//    )
//    val topics = Set("0702")
//
//    // StringDecoder: 第一个key的反序列方式  第二个value的反序列的方式
//    val inputDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//
//    //第二个是真实数据
//    val mapDStream = inputDStream.map(x => x._2)
//
//    mapDStream.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
