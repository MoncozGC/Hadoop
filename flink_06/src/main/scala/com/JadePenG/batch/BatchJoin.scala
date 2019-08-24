package com.JadePenG.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Join算子 合并
  * score.csv ，一个为 subject.csv, 分别保存了成绩数据以及学科数据。
  * 需要将这两个数据连接到一起，然后打印出来。
  *
  * @author Peng
  */
object BatchJoin {

  /**
    * 1. 两个文件复制到项目中的 data 中
    * 2. 构建批处理环境
    * 3. 创建两个样例类
    * 学科Subject（学科ID、学科名字）
    * 成绩Score（唯一ID、学生姓名、学科ID、分数——Double类型）
    * 4. 分别使用 readCsvFile 加载csv数据源，并制定泛型
    * 5. 使用join连接两个DataSet，并使用 where 、 equalTo 方法设置关联条件
    * 6. 打印关联后的数据源
    */

  //创建两个样例类
  case class Subject(id: Int, name: String)

  //
  case class Score(id: Int, scoreName: String, subjectID: Int, score: Double)

  def main(args: Array[String]): Unit = {


    //1. 初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //2. 读取文件
    val subjectDataSet: DataSet[Subject] = env.readCsvFile[Subject]("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\subject.csv")
    val scoreDataSet: DataSet[Score] = env.readCsvFile[Score]("G:\\Develop\\Idea\\Bigdata_01\\flink_06\\data\\score.csv")

    //3. 关联表
    //scoreDataSet表-->对应subjectID     subjectDataSet表对应-->id
    val resultDataSet: JoinDataSet[Score, Subject] = scoreDataSet.join(subjectDataSet).where(_.subjectID).equalTo(_.id)

    resultDataSet.print()
  }
}
