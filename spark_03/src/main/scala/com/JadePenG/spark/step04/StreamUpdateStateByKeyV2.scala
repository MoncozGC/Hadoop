package com.JadePenG.spark.step04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 累计统计词频出现次数(当前批次 + 历史批次)  [最终版本]
  *
  * 优点:
  * 可以容错, 将数据结果及逻辑缓存起来
  * 缺点:
  *   1. 两次checkpoint之间，程序挂掉，数据会丢失
  *   2. 效率较低 产生数据结果时, 多次的写磁盘(hdfs)
  *
  *     解决:
  *       将数据写入到redis mysql
  *
  * @author Peng
  */
object StreamUpdateStateByKeyV2 {

  //设置日志级别
  Logger.getLogger("org").setLevel(Level.WARN)

  //设置缓存路径
  val cachePath = "./spark_03/cache02"

  def main(args: Array[String]): Unit = {
    //getOrCreate: 要么从检查点数据重新创建一个StreamingContext，要么创建一个新的StreamingContext。
    val ssc: StreamingContext = StreamingContext.getOrCreate(cachePath, creatingFun)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 主要作为逻辑处理, 会将所有的逻辑以及数据序列化缓存起来
    *
    * getOrCreate: 要么从检查点数据重新创建一个StreamingContext，要么创建一个新的StreamingContext。
    */
  def creatingFun(): StreamingContext = {
    println("第一次创建")

    val conf = new SparkConf().setMaster("local[*]").setAppName("Cache02")
    val ssc = new StreamingContext(conf, Seconds(5))

    //读取缓存路径
    ssc.checkpoint(cachePath)

    //获取数据源
    val textDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    //5s缓存一次
    textDStream.checkpoint(Seconds(5))

    //处理数据
    val resultDStream: DStream[(String, Int)] = textDStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFun)

    //打印
    resultDStream.print()
    //返回ssc
    ssc
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
