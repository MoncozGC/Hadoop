package com.JadePenG.spark.step03

import org.apache.spark.sql.SparkSession

/**
  *编写Spark SQL程序操作HiveContext
  *
  * @author Peng
  */
object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName)
      .enableHiveSupport() //开启对hive的支持，可以让我们无缝的使用hive的hql语法操作数据
      .getOrCreate()

    val context = spark.sparkContext
    //创建一张hive的表
    spark.sql("create table if not exists student(id int, name string, age int) row format delimited fields terminated by ','")
    //加载student.csv
    spark.sql("load data local inpath './datas/student.csv' overwrite into table student ")

    //查询年龄大于30岁的人
    spark.sql("select * from student where age > 30").show()

    context.stop()
    spark.close()
  }
}
