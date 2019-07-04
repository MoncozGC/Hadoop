package com.JadePenG.spark.step03

import org.apache.spark.sql.SparkSession

/**
  * 通过反射配合样例类推断Schema
  *
  * @author Peng
  */
object CaseClassToDF {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val context = spark.sparkContext
    //设置打印的信息
    context.setLogLevel("WARN")

    //读取文件
    val file = context.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day06_Spark Sql\\资料\\person.txt")

    //通过空格切割
    val arrayRDD = file.map(_.split(" "))

    //将RDD转换成样例类的RDD
    val personRDD = arrayRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))

    //必须要导入隐式转换包才可以将RDD转换成DataFrameRDD
    import spark.implicits._

    //转换成DataFrame
    val personDF = personRDD.toDF()

    //df操作
    personDF.printSchema()
    personDF.select("name").show()
    //查询年龄大于30岁的人
    personDF.filter($"age" > 30).show()
    //按年龄进行分组并统计相同年龄的人数
    personDF.groupBy("age").count().show()

    //将dataFrame注册成一张临时表(也可以注册成临时视图)
    personDF.registerTempTable("t_person")
    //personDF.createOrReplaceGlobalTempView("v_person")

    spark.sql("select * from t_person").show()

    context.stop()
    spark.close()

  }
}
