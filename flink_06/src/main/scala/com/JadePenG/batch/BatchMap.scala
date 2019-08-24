package com.JadePenG.batch

/**
  * Map算子
  * 需求:
  * 使用map操作, 将以下数据
  * "1,张三", "2,李四", "3,王五", "4,赵六"
  * 转换成一个scala的样例类
  *
  * @author Peng
  */
object BatchMap {
  def main(args: Array[String]): Unit = {

    //4. 创建样例类
    case class use(id: Int, name: String)


    import org.apache.flink.api.scala._
    //1. 读取flink的运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2. 读取数据转发成dataset

    val textDataSet = env.fromCollection(
      List("1,张三", "2,李四", "3,王五", "4,赵六")
    )
    //3. 使用map转发成样例类
    val word = textDataSet.map(record => {
      val fields = record.split(",")
      use(fields(0).toInt, fields(1))
    })

    word.print()
  }
}
