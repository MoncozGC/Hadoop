package com.JadePenG.spark.step02.Practice

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map、filter算子操作
  *
  * @author Peng
  */
object MapFilter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapFilter").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    val rdd2 = rdd1.map(_ * 2).sortBy(x => x)
    val rdd3 = rdd2.filter(_ >= 5)
    rdd3.collect()
    println(rdd3)
  }
}
