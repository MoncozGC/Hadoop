package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Peng
  */
object MapPartitionsWithIndexDemo {
  def partitionsFun(index: Int, iter: Iterator[Int]): Iterator[Int] = {
    println("[" + index + "]:" + iter.mkString(","))
    iter
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 5, 2)

    rdd.mapPartitionsWithIndex(partitionsFun).collect()

    sc.stop
  }
}
