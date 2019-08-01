package com.JadePenG.spark.step04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时计算中需要过滤测试用户
  * 游戏中分为两类用户：一类是正式用户，一类是测试用户（测试人员）
  * 那么这个时候，测试用户在游戏中的消费是不能计算到公司的收入，也就是说需要将这部分用户给过滤掉
  *
  * @author Peng
  */
object TransformUserList {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //创建sparkConf对象

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    //首先需要一份测试用户名单：
    val testUserList: Array[(String, Boolean)] = Array(("zhangsan", true), ("lisi", true))
    val testUserRDD: RDD[(String, Boolean)] = ssc.sparkContext.makeRDD(testUserList)

    //读取访问日志
    val logDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //获取数据
    val splitDStream: DStream[(String, String)] = logDStream.map(log => (log.split(" ")(1), log))

    val resultDStream: DStream[String] = splitDStream.transform(rdd => {
      val joinRDD: RDD[(String, (String, Option[Boolean]))] = rdd.leftOuterJoin(testUserRDD)

      joinRDD.foreach(print(_))

      val filterRDD: RDD[(String, (String, Option[Boolean]))] = joinRDD.filter(log => {
        if (log._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      filterRDD.map(log => (log._2._1))
    })

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
