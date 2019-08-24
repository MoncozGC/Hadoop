package com.JadePenG.batch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * 按照指定的key进行hash分区
  *
  * 基于以下列表数据来创建数据源，并按照hashPartition进行分区，然后输出到文件。
  * List(1,1,1,1,1,1,1,2,2,2,2,2)
  *
  * @author Peng
  */
object BatchHashPartition {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 构建批处理运行环境
      * 2. 设置并行度为 2
      * 3. 使用 fromCollection 构建测试数据集
      * 4. 使用 partitionByHash 按照字符串的hash进行分区
      * 5. 调用 writeAsText 写入文件到 data/partition_output 目录中
      * 6. 打印测试
      */
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为 2 核数是2 分区是2
    env.setParallelism(2)
    //3. 使用 fromCollection 构建测试数据集
    val numDataSet = env.fromCollection(
      List(1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2)
    )

    //4. 使用 partitionByHash 按照字符串的hash进行分区
    val partitionDataSet = numDataSet.partitionByHash(_.toString)

    //5. 调用 writeAsText 写入文件到 data/partition_output 目录中
    partitionDataSet.writeAsText("F:\\out")

    env.execute()
  }
}
