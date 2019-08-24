package com.JadePenG.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
  *
  * withBroadcastSet
  * 用法
  * 在需要使用广播的操作后，使用 withBroadcastSet 创建广播
  * 在操作中，使用getRuntimeContext.getBroadcastVariable [广播数据类型] ( 广播名 )获取广播变量
  *
  *
  * 创建一个 学生 数据集，包含以下数据
  * |学生ID | 姓名 |
  * |------|------|
  * List((1, "张三"), (2, "李四"), (3, "王五"))
  * 将该数据，发布到广播。
  * 再创建一个 成绩 数据集，
  * |学生ID | 学科 | 成绩 |
  * |------|------|-----|
  * List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
  * 请通过广播获取到学生姓名，将数据转换为
  * List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
  *
  * @author Peng
  */
object BatchBroadcast {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取批处理运行环境
      * 2. 分别创建两个数据集
      * 3. 使用 RichMapFunction 对 成绩 数据集进行map转换
      * 4. 在数据集调用 map 方法后，调用 withBroadcastSet 将 学生 数据集创建广播
      * 5. 实现 RichMapFunction
      * 将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
      * 重写 open 方法中，获取广播数据
      * 导入 scala.collection.JavaConverters._ 隐式转换
      * 将广播数据使用 asScala 转换为Scala集合，再使用toList转换为scala List 集合
      * 在 map 方法中使用广播进行转换
      * 6. 打印测试
      */
    //1. 获取批处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2. 分别创建两个数据集
    //学生id  学生姓名
    val studentDataSet: DataSet[(Int, String)] = env.fromCollection(
      List((1, "张三"), (2, "李四"), (3, "王五"))
    )
    //学生id  学科 成绩
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(
      List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86))
    )

    //3. 使用 RichMapFunction 对成绩数据集进行map转换
    //将学生表广播(小数据集) 成绩表相对来说(大数据集)  传进去的是: (1, "语文", 50)   返回的是: (学生id, 学生姓名, 成绩)
    //转换成的数据==>  List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
    val resultDataSet: DataSet[(String, String, Int)] = scoreDataSet.map(mapper = new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      var studentList: List[(Int, String)] = _

      /**
        * 重写open方法, 该方法只被执行一次, 类似于初始化,因此可以再这里获取广播变量
        *
        * @param parameters
        */
      override def open(parameters: Configuration) = {
        //将java的List集合转发成scan的集合
        import scala.collection.JavaConversions._
        //在上下文中取广播变量
        studentList = getRuntimeContext.getBroadcastVariable[(Int, String)]("student").toList
      }

      /**
        * 会被多次执行, 集合中有多少数据就被执行多少次, 再这里读取广播变量的数据不合适
        *
        * 用户名 学生名
        *
        * @param value
        * @return
        */
      override def map(value: (Int, String, Int)): (String, String, Int) = {
        //获取学生ID  (1  传进去的是: (1, "语文", 50)
        val studentId = value._1
        //获取学生名字   根据学生Id取学生姓名
        //学生姓名在广播变量中, 获取广播变量中第一个值(ID) == studentID 就是同一个人,再获取集合中的第二个参数
        val studentName = studentList.filter(_._1 == studentId)(0)._2
        //获取学生成绩   50)

        //直接返回
        (studentName, value._2, value._3)
      }

      //将学生数据集广播, 并给它取名字  广播的数据(学生ID, 学生姓名)
    }).withBroadcastSet(studentDataSet, "student")

    resultDataSet.print()
  }
}
