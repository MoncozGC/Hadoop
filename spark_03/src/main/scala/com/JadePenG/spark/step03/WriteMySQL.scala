package com.JadePenG.spark.step03

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 将person.txt中的数据写入到mysql中
  *
  * @author Peng
  */
object WriteMySQL {

  case class Person(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    val context = spark.sparkContext
    context.setLogLevel("WARN")

    val fileRDD = context.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day06_Spark Sql\\资料\\person.txt")

    val arrayRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))

    val personRDD: RDD[Person] = arrayRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))

    import spark.implicits._

    val personDF = personRDD.toDF()

    personDF.createOrReplaceTempView("v_person")

    val result = spark.sql("select * from v_person")

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "0712")

    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://127.0.0.1/spark", "v_person", props)

    context.stop()
    spark.stop()

  }
}
