package com.JadePenG.report

import com.JadePenG.etl.ETLRunner
import com.JadePenG.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.SparkSession

/**
  * 按照地域统计广告投放的分布情况
  */
object RegionReportRunner {

  def main(args: Array[String]): Unit = {
    import com.JadePenG.utils.ConfigHelper._
    import com.JadePenG.utils.KuduHelper._

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("region_report")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()
    import spark.implicits._

    // 2. 读取数据集
    val source = spark.readKuduTable(ETLRunner.ODS_TABLE)
    if (source.isEmpty) return
    val sourceDF = source.get

    // 3. 处理
    // ip, region, city
    import org.apache.spark.sql.functions._

    // 什么时候使用命令式 API, 缺点: 比较繁琐啰嗦, 优点: 逻辑非常清晰
    // 什么时候使用 SQL
    // 缺点: select * from (select * from (select name,age...)) 不适合比较复杂的处理和查询
    // 优点: SQL 针对结果进行描述, select name, age, count(*) as count, 比较简洁
    val result = sourceDF.select('region, 'city)
      .groupBy('region, 'city)
      .agg(count("*") as "count")

    // 4. 落地数据集
    // 4.1 创建 Kudu 表
    spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
    // 4.2 落地数据
    result.writeToKudu(TARGET_TABLE_NAME)
  }

  private val TARGET_TABLE_NAME = "report_runner_" + KuduHelper.FORMAT_DATE

  import scala.collection.JavaConverters._
  private val schema = new Schema(
    List(
      new ColumnSchemaBuilder("region", Type.STRING).key(true).nullable(false).build(),
      new ColumnSchemaBuilder("city", Type.STRING).key(true).nullable(false).build(),
      new ColumnSchemaBuilder("count", Type.INT64).key(false).nullable(true).build()
    ).asJava
  )

  private val keys = Seq("region", "city")
}
