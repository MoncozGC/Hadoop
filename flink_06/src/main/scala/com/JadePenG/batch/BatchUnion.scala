package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 将两个DataSet取并集，并自动进行去重。 需加distinct()去重
  *
  * 数据集1
  * "hadoop", "hive", "flume"
  * 数据集2
  * "hadoop", "hive", "spark"
  *
  * @author Peng
  */
object BatchUnion {
  /**
    * 1. 构建批处理运行环境
    * 2. 使用 fromCollection 创建两个数据源
    * 3. 使用 union 将两个数据源关联在一起
    * 4. 打印测试
    */
  def main(args: Array[String]): Unit = {
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    //2. 使用 fromCollection 创建两个数据源
    val wordDataSet1 = env.fromCollection(
      List("hadoop", "hive", "flume")
    )
    val wordDataSet2 = env.fromCollection(
      List("hadoop", "hive", "spark")
    )

    //3. 使用 union 将两个数据源关联在一起
    val resultDataSet = wordDataSet1.union(wordDataSet2).distinct()

    val e1 = env.fromElements("123")
    val e2 = env.fromElements("456")
    val e3 = env.fromElements("123")

    resultDataSet.print()
  }
}
