package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Peng
  */
object AggregateByKeyDemo {
  def partitionsFun(index: Int, iter: Iterator[(Int, Int)]): Iterator[(Int, Int)] = {
    println("[" + index + "]:" + iter.mkString(","))
    iter
  }

  def partitionsFun2(index: Int, iter: Iterator[String]): Iterator[String] = {
    println("[" + index + "]:" + iter.mkString(","))
    iter
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1, 2), (2, 3), (3, 6), (3, 4), (3, 5), (4, 2), (4, 5)), 3)

    rdd.mapPartitionsWithIndex(partitionsFun).collect()

    rdd.aggregateByKey(10)((x, y) => Math.max(x, y), (x, y) => Math.max(x, y)).collect().foreach(println(_))

    val func = (index: Int, it: Iterable[String]) => it.map(s"index:${index},ele:" + _)
    val rdd2 = sc.makeRDD(Array("a", "b", "c", "d", "e", "f"), 2)
    rdd2.mapPartitionsWithIndex(partitionsFun2).collect()

    rdd2.aggregate("*")(_ + _, _ + _).foreach(println(_))
  }
}
