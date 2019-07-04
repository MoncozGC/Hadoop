package com.JadePenG.spark.step03

import com.JadePenG.spark.step03.CaseClassToDF.Person
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 通过StructType配合Row直接指定Schema
  *
  * @author Peng
  */
object StructTypeToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val context: SparkContext = spark.sparkContext
    context.setLogLevel("WARN")

    val file: RDD[String] = context.textFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day06_Spark Sql\\资料\\person.txt")

    val arrRDD: RDD[Array[String]] = file.map(_.split(" "))

    val personRDD = arrRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))

    val rowRDD = arrRDD.map(x=>Row(x(0).toInt, x(1), x(2).toInt))

    val struct = new StructType().add("id", IntegerType, false)
      .add("name", StringType, true)
      .add("age", IntegerType, true)

    val dataFrame = spark.createDataFrame(rowRDD, struct)

    dataFrame.createTempView("S_person")
    dataFrame.show()

    dataFrame.select("name").show()
    spark.sql("select * from S_person").show()

    spark.close()
    context.stop()
  }

}
