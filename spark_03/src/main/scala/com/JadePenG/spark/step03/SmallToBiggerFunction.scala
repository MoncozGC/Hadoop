package com.JadePenG.spark.step03

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes

/**
  * 将文件中的内容转换成大写
  *
  * @author Peng
  */
object SmallToBiggerFunction {

  case class Small(line: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val fileRdd: RDD[String] = spark.sparkContext.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day06_Spark Sql\\资料\\udf.txt")

    val smallRDD: RDD[Small] = fileRdd.map(x => Small(x))

    //导入隐式转换
    import spark.implicits._
    //转换成视图并注册成表
    smallRDD.toDF().createOrReplaceTempView("small_table")

    //通过使用sparkSession进行udf注册，将字符串转换成大写
    spark.udf.register("small2Bigger", (line: String) => {
      line.toUpperCase
    })

    spark.udf.register("smallToBigger", new UDF1[String, String]() {
      override def call(t1: String): String = {
        t1.toUpperCase
      }
    }, DataTypes.StringType)

    spark.sql("select line, small2Bigger(line) from small_table").show()

    spark.stop()
  }
}

