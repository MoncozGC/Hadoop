package com.JadePenG.spark.step01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ScalaWordCount
  *
  * 传递两个参数, 一个源文件的路径, 一个数据保存路径
  *
  * @author Peng
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    /**
      * SparkContext是我们的程序入口，如果想开发SparkCore应用程序首先需要创建SparkContext对象
      * 主要是实例化了两个对象：
      * 1）DAGScheduler
      * 2）TaskScheduler
      */
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //将传入的参数赋值给变量
    //val inputPath = args(0)
    //val outputPath = args(1)
    //val Array(inputPath, outputPath) = args

    //通过sc读取数据，指定数据源
    val lines: RDD[String] = sc.textFile("F:\\wordCount.txt")
    lines.cache()

    //将内容分词压平
    val words: RDD[String] = lines.flatMap(_.split(" ")).filter(!_.isEmpty)

    //每个单词记一次数
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)

    //将数据保存到指定路径

    sorted.saveAsTextFile("F:\\text")
    //    val result: Array[(String, Int)] = sorted.collect()
    //    result.foreach(println(_))

    //释放资源
    sc.stop()
  }
}
