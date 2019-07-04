package com.JadePenG.spark.step04

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过使用mysql累计单词出现的次数
  *
  * @author Peng
  */
object MyNetworkWordCount {
  def main(args: Array[String]): Unit = {

    //创建conf
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    //创建一个Streaming对象每两秒拉取一次数据
    val ssc = new StreamingContext(conf, Seconds(2))

    //创建一个离散流(DStream), DStream表示的数据流 离散流是由一个个RDD组成
    val lines = ssc.socketTextStream("192.168.25.112", 9999)

    //处理数据
    val words = lines.flatMap(_.split(" "))

    //返回结果
    val result = words.map((_,1)).reduceByKey(_+_)

    //输出结果
    result.print()

    //启动StreamingContext， 开始执行计算
    ssc.start()
    //等待计算完成，主程序阻塞，不能死掉
    ssc.awaitTermination()


  }
}
