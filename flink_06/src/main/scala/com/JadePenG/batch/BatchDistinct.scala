package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  *Distinct算子 去重
  * 请将以下元组数据，使用 distinct 操作去除重复的单词
  * ("java" , 1) , ("java", 1) ,("scala" , 1)
  * 去重得到
  * ("java", 1), ("scala", 1)
  *
  * @author Peng
  */
object BatchDistinct {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取 ExecutionEnvironment 运行环境
      * 2. 使用 fromCollection 构建数据源
      * 3. 使用 distinct 指定按照哪个字段来进行去重
      * 4. 打印测试
      */
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //2. 构建本机集合
    val textDataSet = env.fromCollection(
      List(("java", 1), ("java", 1), ("scala", 1),("scala", 2),("scala", 2))
    )

    //3. 使用 distinct 指定按照哪个字段来进行去重
    val resultDataSet = textDataSet.distinct()

    resultDataSet.print()
  }
}
