package com.JadePenG.spark.step02

import java.util.Properties

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 在mysql中读取数据创建DataFrame
  *
  * @author Peng
  */
object ReadMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "0712")
    val jdbcDF = spark.read.jdbc("jdbc:mysql://192.168.82.110:3306/spark", "t_student", props)

    jdbcDF.show()
    spark.stop()
  }


}
