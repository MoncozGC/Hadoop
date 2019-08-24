package com.JadePenG.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Aggregate算子 按照内置的方式来进行聚合。例如：SUM/MIN/MAX..
  * 请将以下元组数据，使用 aggregate 操作进行单词统计
  * ("java" , 1) , ("java", 1) ,("scala" , 1)
  *
  * @author Peng
  */
object BatchAggregate {
  /**
    * 1. 获取 ExecutionEnvironment 运行环境
    * 2. 使用 fromCollection 构建数据源
    * 3. 使用 groupBy 按照单词进行分组
    * 4. 使用 aggregate 对每个分组进行 SUM 统计
    * 5. 打印测试
    */
  def main(args: Array[String]): Unit = {
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //2. 构建本机集合
    val textDataSet: DataSet[(String, Int)] = env.fromCollection(
      List(("java", 1), ("java", 1), ("scala", 1))
    )

    //3. 使用 groupBy 按照单词进行分组
    val groupDataSet: GroupedDataSet[(String, Int)] = textDataSet.groupBy(0)

    //4. 使用 aggregate 对每个分组进行 SUM 统计
    val resultDataSet = groupDataSet.aggregate(Aggregations.SUM, 1)

    resultDataSet.print()
  }
}
