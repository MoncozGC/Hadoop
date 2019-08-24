package com.JadePenG.batch

/**
  * MapPartition算子
  * 使用mapPartition操作，将以下数据
  * "1,张三", "2,李四", "3,王五", "4,赵六"
  * 转换为一个scala的样例类。
  *
  * @author Peng
  */
object BatchMapPartition {

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
    val word = textDataSet.mapPartition(partition => {
      //初始化外部资源(redis,mysql)
      partition.map(element => {
        val fields = element.split(",")
        use(fields(0).toInt, fields(1))
      })
    })

    word.print()
  }
}


