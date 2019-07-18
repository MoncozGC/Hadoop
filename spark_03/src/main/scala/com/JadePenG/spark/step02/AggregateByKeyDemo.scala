package com.JadePenG.spark.step02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * kAggregateByKey算子
  *
  * 在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
  * seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分区中的结果
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
