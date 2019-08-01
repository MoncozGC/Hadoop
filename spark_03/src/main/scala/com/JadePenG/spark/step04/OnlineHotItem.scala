package com.JadePenG.spark.step04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求：计算一下每20秒统计一次过去60秒的热搜
  * 返回数据格式：关键字
  * 返回数据要求：热词排行前三
  *
  * @author Peng
  */
object OnlineHotItem {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HostOn")
    //每5秒拉取一次数据
    val ssc = new StreamingContext(conf, Seconds(5))

    //获取数据  2019 hadoop
    val textDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //对数据进行拆分 --> isEmpty() --> 统计次数
    val filterDStream: DStream[(String, Int)] = textDStream.map(_.split(" ")(1)).filter(!_.isEmpty).map((_, 1))

    //每20秒统计前面60秒的数据
    val windowDStream: DStream[(String, Int)] = filterDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(20), Seconds(60))

    val resultDStream: DStream[(String, Int)] = filterDStream.transform(rdd => {
      //根据value排序 升序
      val sort = rdd.sortBy((_._2), false).take(3)
      ssc.sparkContext.makeRDD(sort)
    })

    resultDStream.print()
    ssc.start()
    ssc.awaitTermination()


  }
}
