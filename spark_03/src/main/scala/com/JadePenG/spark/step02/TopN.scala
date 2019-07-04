package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算TopN
  *
  * @author Peng
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val file = sc.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\运营商日志\\access.log")

    val ips = file.map(_.split("\\|")).map(x => x(1))
    val reduced = ips.map(x => (x, 1)).reduceByKey(_ + _)

    val sorted = reduced.sortBy(_._2, false)
    val topN = sorted.take(5)
    topN.foreach(println(_))
    sc.stop()
  }

}
