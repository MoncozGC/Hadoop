package com.JadePenG.spark.step04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 累计统计词频出现次数(当前批次 + 历史批次)
  *
  * @author Peng
  */
object StreamUpdateStateByKeyV1 {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Stream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    /**
      * 需要设置checkpoint目录，缓存历史批次数据
      * Metadata checkpoint: 将定义的streaming计算的信息保存到容错存储里面（hdfs）有利于从运行streaming应用程序的driver端进行恢复
      * Data checkpoint：将生成的rdd保存到hdfs上
      */
    ssc.checkpoint("./spark_03/cache01")

    //接收数据
    val textFile: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //处理数据
    val resultDStream: DStream[(String, Int)] = textFile.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFun)

    resultDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * updateStateByKey 定义的函数
    *
    * @param newValue  当前批次的某个单词出现的值的序列集合(hadoop=>Seq[Int](1,1,1,1,1,1,1,1,1,1))
    * @param nextValue 历史批次跑出来的某个单词数量的总值(hadoop=>Option[Some(10)])
    * @return
    */
  def updateFun(newValue: Seq[Int], nextValue: Option[Int]): Option[Int] = {
    val newCount = newValue.sum + nextValue.getOrElse(0)
    Option(newCount)
  }
}
