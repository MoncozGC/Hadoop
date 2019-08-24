package com.JadePenG.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * 指定字段对分区中的数据进行排序
  *
  * 按照以下列表来创建数据集 是根据字典排序
  * List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink")
  * 对分区进行排序后，输出到文件。
  *
  * @author Peng
  */
object BatchSortPartition {
  /**
    * 1. 构建批处理运行环境
    * 2. 使用 fromCollection 构建测试数据集
    * 3. 设置数据集的并行度为 2
    * 4. 使用 sortPartition 按照字符串进行降序排序
    * 5. 调用 writeAsText 写入文件到 data/sort_output 目录中
    * 6. 启动执行
    */

  def main(args: Array[String]): Unit = {
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为 2 核数是2 分区是2
    //并行度优先级: 算子>全局>配置文件
    env.setParallelism(2)
    //3. 使用 fromCollection 构建测试数据集
    val numDataSet = env.fromCollection(
      List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink")
    )
    //4. 使用 sortPartition 按照字符串进行降序排序
    numDataSet.sortPartition(_.toString, Order.DESCENDING)

    //5. 调用 writeAsText 写入文件到 data/sort_output 目录中
    numDataSet.writeAsText("F:\\AA")

    env.execute()
  }

}
