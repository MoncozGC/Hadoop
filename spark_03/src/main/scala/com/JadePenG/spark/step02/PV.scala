package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算PV
  *
  * @author Peng
  */
object PV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val file = sc.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\运营商日志\\access.log")

    //每一个val添加1
    val pvAndOne = file.map(x => ("pv", 1))
    //val累加
    val totalPV = pvAndOne.reduceByKey(_ + _)
    totalPV.foreach(println(_))
    sc.stop()
  }

}
