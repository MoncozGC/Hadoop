package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 特殊的宽依赖
  *
  * @author Peng
  */
object WideDepend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(("hello", 1), ("kitty", 1), ("hello", 2), ("kitty", 2)), 2)
    val rdd2 = sc.parallelize(Array(("hello", 3), ("kitty", 3), ("hello", 4), ("kitty", 4)), 2)

    val rdd3 = rdd1.join(rdd2, 2)
    val func = (index: Int, it: Iterator[(String, (Int, Int))]) => {
      println(s"index:${index}, it${it.toList.mkString(",")}")
      it
    }
    val result = rdd3.mapPartitionsWithIndex(func)
    result.count()
    sc.stop()
  }

}
