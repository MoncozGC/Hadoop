package com.JadePenG.utils

import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.time.FastDateFormat
import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object KuduHelper {
  private val config = ConfigFactory.load("kudu")
  val MASTER: String = config.getString("kudu.common.master")
  val ODS_PREFIX: String = config.getString("kudu.name.pmt_ods")

  implicit def sparkSessionToKuduHelper(sparkSession: SparkSession): SparkSessionKuduHelper = {
    new SparkSessionKuduHelper(sparkSession)
  }

  implicit def dataFrameToKuduHelper(dataFrame: DataFrame): DataFrameKuduHelper = {
    new DataFrameKuduHelper(dataFrame)
  }

  def FORMAT_DATE(): String = {
    FastDateFormat.getInstance("yyyyMMdd").format(new Date)
  }
}

/**
  * spark.read
  * spark.create
  * spark.write
  *
  * dataFrame.read
  */
//class KuduHelper {
//
//}

/**
  * 包装 SparkSession, 提供创建表和读取数据的功能
  * 限定 create 和 read 只能在 SparkSession 上调用
  */
class SparkSessionKuduHelper(spark: SparkSession) {
  val kuduContext = new KuduContext(kuduMaster = KuduHelper.MASTER, sc = spark.sparkContext)

  def createKuduTable(tableName: String, schema: Schema, keys: Seq[String]): KuduTable = {
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }

    import scala.collection.JavaConverters._
    val options = new CreateTableOptions()
        .addHashPartitions(keys.asJava, 6)
        .setNumReplicas(1)

    kuduContext.createTable(tableName, schema, options)
  }

  /**
    * 读取 Kudu 表, 返回 DataFrame
    *
    * 在 Scala 中, 表示一个东西有或者没有, 用什么?
    */
  def readKuduTable(tableName: String): Option[DataFrame] = {
    import org.apache.kudu.spark.kudu._

    // 判定是否存在这张表
    if (kuduContext.tableExists(tableName)) {
      val source = spark.read
        .option("kudu.master", KuduHelper.MASTER)
        .option("kudu.table", tableName)
        .kudu
      Some(source)
    } else {
      None
    }
  }
}

/**
  * 包装 DataFrame, 提供写入 Kudu 的功能
  * 限定 write kudu 只能在 DataFrame 上调用
  */
class DataFrameKuduHelper(dataFrame: DataFrame) {

  /**
    * 将 DataFrame 的数据写入 Kudu 表
    */
  def writeToKudu(tableName: String): Unit = {
    import org.apache.kudu.spark.kudu._

    dataFrame.write
      .option("kudu.master", KuduHelper.MASTER)
      .option("kudu.table", tableName)
      .mode(SaveMode.Append)
      .kudu
  }
}
