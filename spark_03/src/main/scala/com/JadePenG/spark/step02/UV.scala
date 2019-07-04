package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算UV
  *
  * @author Peng
  */
object UV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val file = sc.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day05_Spark RDD\\资料\\运营商日志\\access.log")
    val ips = file.map(_.split("\\|")).map(x => x(1))
    ips.foreach(println(_))
    val uvAndOne = ips.distinct().map(x => ("UV", 1))
    val totalUV = uvAndOne.reduceByKey(_ + _)
    totalUV.foreach(println(_))

    sc.stop()
  }

}
