package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * 统计单词数量
  *
  * @author Peng
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1. 获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2. 构建一个collection源
    //需要导入Flink的隐式包
    import org.apache.flink.api.scala._
    val wordDataSet: DataSet[String] = env.fromCollection {
      List("hadoop hive spark", "fink mr hbase", "spark hive flume")
    }

    //3. 使用flatMap将字符串进行切割后扁平化
    val words: DataSet[String] = wordDataSet.flatMap(_.split(" "))

    //4. 将单词转换为(单词, 数量)的元组
    val wordNumDataSet: DataSet[(String, Int)] = words.map(x => (x, 1))

    //5. 将第一个字段进行分组  0: 是元素的下标 String
    val wordGroupDataSet: GroupedDataSet[(String, Int)] = wordNumDataSet.groupBy(0)

    //6. 统计  对第二个元素进行统计 Int
    val wordCountDataSet: AggregateDataSet[(String, Int)] = wordGroupDataSet.sum(1)

    //7. 打印
    wordCountDataSet.print()


  }

}
