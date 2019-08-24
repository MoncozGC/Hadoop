package com.JadePenG.batch


import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

import scala.io.Source


/**
  * 分布式缓存: 数据量大,比TaskManager的空闲内存更大的时候可以使用, 反之使用广播变量
  *
  * 创建一个 成绩 数据集
  * List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
  * 请通过分布式缓存获取到学生姓名，将数据转换为
  * List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
  *
  * @author Peng
  */
object BatchDistributionCache {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 将 distribute_cache_student 文件上传到HDFS / 目录下 注册分布式缓存,
      * 2. 获取批处理运行环境
      * 3. 创建成绩数据集
      * 4. 对 成绩 数据集进行map转换，将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
      * RichMapFunction 的 open 方法中，获取分布式缓存数据
      * 在 map 方法中进行转换
      * 5. 实现 open 方法
      * 使用 getRuntimeContext.getDistributedCache.getFile 获取分布式缓存文件
      * 使用 Scala.fromFile 读取文件，并获取行
      * 将文本转换为元组（学生ID，学生姓名），再转换为List
      * 6. 实现 map 方法
      * 从分布式缓存中根据学生ID过滤出来学生
      * 获取学生姓名
      * 构建最终结果元组
      * 7. 打印测试
      */
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2. 将文件上传到hdfs上面，注册分布式缓存，将缓存文件赋值到各个TaskManager节点
    env.registerCachedFile("hdfs://node01:8020/myText/flink/distribute_cache_student", "student")

    //3. 创建成绩数据源
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(
      List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86))
    )

    //4. 对 成绩 数据集进行map转换，将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
    val resultDataSet = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      //将分布式缓存中的数据放到list集合中, 如果分布式缓存文件中数据量非常大，那么不能直接将数据toList(OOM)
      var studentList: List[(Int, String)] = _

      //RichMapFunction 的 open 方法中，获取分布式缓存数据
      override def open(parameters: Configuration): Unit = {
        //读取一个文件
        val file: File = getRuntimeContext.getDistributedCache.getFile("student")
        //读取文件内容
        val lineIter: Iterator[String] = Source.fromFile(file).getLines()
        //将文本内容转换成元组对象, 将 1, 张三转换成元组对象(学生ID, 学生名字)
        studentList = lineIter.map(line => {
          val fields: Array[String] = line.split(",")
          //学生id, 学生姓名
          (fields(0).toInt, fields(1))
          //数据量不大可以使用toList, 但是数据量太大不行 08:49
        }).toList
      }

      //执行多次, 数据集中有多少数据就执行多少次
      override def map(value: (Int, String, Int)): (String, String, Int) = {
        val studentId = value._1
        val studentName = studentList.filter(_._1 == studentId)(0)._2
        //将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
        (studentName, value._2, value._3)
      }
    })
    //6：打印测试
    resultDataSet.print()
  }
}
