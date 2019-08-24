package com.JadePenG.kudu

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.{Schema, Type, client}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.junit.Test

class SparkKuduTest {
  private val TABLE_NAME = "simple5"

  @Test
  def sparkReview(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("kudu")
      .master("local[6]")
      .getOrCreate()

    // 2. 创建 KuduContext
    val masters = List("cdh01:7051", "cdh02:7051", "cdh03:7051").mkString(",")
    val kuduContext = new KuduContext(masters, spark.sparkContext)

    // 3. 判断存在和删除表
    if (kuduContext.tableExists(TABLE_NAME)) {
      kuduContext.deleteTable(TABLE_NAME)
    }

    // 4. 创建表
    import scala.collection.JavaConverters._
    val schema = new Schema(
      List(
        new ColumnSchemaBuilder("name", Type.STRING).key(true).nullable(false).build(),
        new ColumnSchemaBuilder("age", Type.INT32).key(false).nullable(true).build(),
        new ColumnSchemaBuilder("gpa", Type.DOUBLE).key(false).nullable(true).build()
      ).asJava
    )

    val options = new CreateTableOptions()
      .addHashPartitions(List("name").asJava, 6)
      .setNumReplicas(3)

    kuduContext.createTable(TABLE_NAME, schema, options)
  }

  @Test
  def dataProcess(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("kudu")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 创建 KuduContext
    val masters = List("cdh01:7051", "cdh02:7051", "cdh03:7051").mkString(",")
    val kuduContext = new KuduContext(masters, spark.sparkContext)

    // 3. 用什么表示数据?
    val dataFrame = Seq(("zhangsan", 10, 4.5), ("lisi", 10, 4.5)).toDF("name", "age", "gpa")

    // 4. 增删改增改
    kuduContext.deleteRows(dataFrame.select("name"), TABLE_NAME)

    kuduContext.insertRows(dataFrame, TABLE_NAME)

    kuduContext.updateRows(dataFrame, TABLE_NAME)

    kuduContext.upsertRows(dataFrame, TABLE_NAME)
  }

  @Test
  def dataFrameReview(): Unit = {
    // 1. SparkSession
    val spark = SparkSession.builder()
      .appName("review")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据集, CSV
    val schema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = true),
        StructField("gpa", DoubleType, nullable = true)
      )
    )

    val source: Dataset[Row] = spark.read
      .option("delimiter", "\t")
      .option("header", value = false)
      .schema(schema)
      .csv("dataset/studenttab10k")

    // Dataset 底层是 RDD, Dataset 底层是什么 RDD[_]?
    // ds.groupby(...).agg(fun...).select(c1, c2, c3) catalyst => ds.select(...).groupby(..).agg(fun...)
    // SQL -> AST 语法树 -> Optimizer -> Logic Plan -> 成本模型 -> 物理计划 -> 集群
    // 因为 SparkSQL 下有一个优化器在执行优化, 所以生成的物理计划就是 RDD, 所有的计划生成的 RDD 是一个统一的类型
    // RDD[InternalRow]
    // 这段代码性能不算差, 但是也有开销, val rdd: RDD[Student] = sourceDS.rdd
    val sourceDS: Dataset[Student] = source.as[Student]

    // 3. 简单处理

    // 4. 写入数据集
    sourceDS.write
      .mode(SaveMode.Overwrite)
      .partitionBy("age")
      .json("dataset/student_json")
  }

  @Test
  def write(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("kudu")
      .master("local[6]")
      .getOrCreate()

    // 2. 创建 KuduContext
    val masters = List("cdh01:7051", "cdh02:7051", "cdh03:7051").mkString(",")
    val kuduContext = new KuduContext(masters, spark.sparkContext)

    // 2. 读取数据
    val schema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = true),
        StructField("gpa", DoubleType, nullable = true)
      )
    )

    val source = spark.read
      .option("delimiter", "\t")
      .option("header", value = false)
      .schema(schema)
      .csv("dataset/studenttab10k")

    // 创建表
    import scala.collection.JavaConverters._
    val options = new client.CreateTableOptions()
      .addHashPartitions(List("name").asJava, 6)
      .setNumReplicas(3)
    kuduContext.createTable("student10", schema, List("name"), options)

    // 3. 写入 Kudu
    import org.apache.kudu.spark.kudu._

    source.write
      .mode(SaveMode.Append)
      .option("kudu.table", "student10")
      .option("kudu.master", masters)
      .kudu
  }

  @Test
  def readKudu(): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("kudu")
      .master("local[6]")
      .getOrCreate()

    // 2. 创建 KuduContext
    val masters = List("cdh01:7051", "cdh02:7051", "cdh03:7051").mkString(",")
    val kuduContext = new KuduContext(masters, spark.sparkContext)

    import org.apache.kudu.spark.kudu._

    val source = spark.read
      .option("kudu.master", masters)
      .option("kudu.table", "student10")
      .kudu

    // 假如在此处做一些操作
    // 因为 Spark SQL 中, 无论是写 SQL 还是写 Dataset 的操作, 其都经过优化器
    // 所以呢会经过一些分析, 得出最终执行计划
    // Kudu 和 Spark 整合的时候, 会参考这些执行计划, 下推到 Kudu 中直接执行
    //    spark.sql("select * from ....")
    //    source.select("")

    source.printSchema()
  }
}

case class Student(name: String, age: Int, gpa: Double)
