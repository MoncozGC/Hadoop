package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * MapPartitionsWithIndex: 可以查看分区中的数据  在数据倾斜的时候可以查看
  *
  * @author Peng
  */
object MapPartitionsWithIndexDemo {
  def partitionsFun(index: Int, iter: Iterator[Int]): Iterator[Int] = {
    //mkString 使用分隔符切割
    println("[" + index + "]:" + iter.mkString(","))
    iter
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //rdd中有两个分区
    val rdd = sc.parallelize(1 to 5, 2)

    rdd.mapPartitionsWithIndex(partitionsFun).collect()

    sc.stop
  }
}
