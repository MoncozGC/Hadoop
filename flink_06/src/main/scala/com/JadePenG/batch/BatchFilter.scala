package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * filter算子 过滤
  * 过滤出来以下以 h 开头的单词。
  * "hadoop", "hive", "spark", "flink"
  *
  * @author Peng
  */
object BatchFilter {
  def main(args: Array[String]): Unit = {
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    //2. 构建本机集合
    val textDataSet: DataSet[String] = env.fromCollection(
      List("hadoop", "hive", "spark", "flink")
    )

    //3. 使用filter算子  只输出h开头的
    val filterDataSet: DataSet[String] = textDataSet.filter(_.startsWith("h"))

    filterDataSet.print()
  }
}
