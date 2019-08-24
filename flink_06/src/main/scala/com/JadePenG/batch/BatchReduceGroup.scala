package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * reduceGroup算子
  * 请将以下元组数据，下按照单词使用 groupBy 进行分组，再使用 reduceGroup 操作进行单词计数
  * ("java" , 1) , ("java", 1) ,("scala" , 1)
  *
  * @author Peng
  */
object BatchReduceGroup {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取 ExecutionEnvironment 运行环境
      * 2. 使用 fromCollection 构建数据源
      * 3. 使用 groupBy 按照单词进行分组
      * 4. 使用 reduceGroup 对每个分组进行统计
      * 5. 打印测试
      */
    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //2. 构建本机集合
    val textDataSet = env.fromCollection(
      List(("java", 1), ("java", 1), ("scala", 1))
    )

    //3. group进行分组 按key进行分组
    val groupDataSet: GroupedDataSet[(String, Int)] = textDataSet.groupBy(0)

    //4. reduceGroup   外面的函数就是对分区聚合后的数据进行拉取聚合
    val group = groupDataSet.reduceGroup(iter => {
      //里面的函数先执行 也就是对分组后的数据先进行聚合操作
      //元组对象第一个参数: 初始值或者临时累加值 第二个参数: 当前最新的元素
      iter.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
    })

    group.print()
  }
}
