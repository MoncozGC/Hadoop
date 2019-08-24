package com.JadePenG.batch

/**
  * FlatMap
  * 张三,中国,江西省,南昌市
  * 李四,中国,河北省,石家庄市
  * Tom,America,NewYork,Manhattan
  *
  * @author Peng
  */
object BatchFlatMap {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    //1. 读取flink的运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2. 读取数据转发成dataset
    val textDataSet: DataSet[String] = env.fromCollection(
      List("张三,中国,江西省,南昌市", "李四,中国,河北省,石家庄市", "Tom,America,NewYork,Manhattan")
    )
    //3. 使用flatMap转发成样例类
    val resultDataSet: DataSet[(String, String)] = textDataSet.flatMap(text => {
      val fields: Array[String] = text.split(",")
      List(
        (fields(0), fields(1)),
        (fields(0), fields(1) + fields(2)),
        (fields(0), fields(1) + fields(2) + fields(3))
      )
    })

    //map打印三个List集合
    val resultMapDataSet: DataSet[List[(String, String)]] = textDataSet.map(text => {
      val fields: Array[String] = text.split(",")
      List(
        (fields(0), fields(1)),
        (fields(0), fields(1) + fields(2)),
        (fields(0), fields(1) + fields(2) + fields(3))
      )
    })
    resultDataSet.print()
    println("---------------------------------")
    resultMapDataSet.print()

  }
}
