package com.JadePenG.spark.step03

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 在MYSQL中读取数据创建DataFrame
  *
  * @author Peng
  */
object ReadMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "0712")
    val jdbcDF: DataFrame = spark.read.jdbc("jdbc:mysql://127.0.0.1/spark", "t_student", props)

    jdbcDF.show()
    spark.close()

  }
}
