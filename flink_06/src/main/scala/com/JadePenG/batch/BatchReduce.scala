package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * reduce算子
  * 请将以下元组数据，使用 reduce 操作聚合成一个最终结果
  * ("java" , 1) , ("java", 1) ,("java" , 1)
  * 将上传元素数据转换为 ("java",3)
  *
  * @author Peng
  */
object BatchReduce {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取 ExecutionEnvironment 运行环境
      * 2. 使用 fromCollection 构建数据源
      * 3. 使用 reduce 执行聚合操作
      * 4. 打印测试
      */
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //2. 构建本机集合
    val textDataSet = env.fromCollection(
      List(("java", 1), ("java", 1), ("java", 1))
    )

    //3. 使用reduce算子执行聚合操作  t1: 累计值 t2: 最新值5
    val reduceDataSet: DataSet[(String, Int)] = textDataSet.reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    reduceDataSet.print()
  }
}
