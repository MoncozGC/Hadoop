package com.JadePenG.kudu

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.{CreateTableOptions, KuduClient, PartialRow}
import org.apache.kudu.{Schema, Type}
import org.junit.{Before, Test}

class KuduTest {
  private var client: KuduClient = null

  @Before
  def createKuduClient(): Unit = {
    val masters = List("cdh01:7051", "cdh02:7051", "cdh03:7051").mkString(",")
    client = new KuduClientBuilder(masters).build()
  }

  @Test
  def clientCreateTable(): Unit = {
    // 1. 创建 Schema
    val name = "simple3"

    // Schema 是表结构
    import scala.collection.JavaConverters._
    val schema: Schema = new Schema(
      List(
        new ColumnSchemaBuilder("name", Type.STRING).key(true).nullable(false).build(),
        new ColumnSchemaBuilder("age", Type.INT32).key(false).nullable(true).build(),
        new ColumnSchemaBuilder("gpa", Type.DOUBLE).key(false).nullable(true).build()
      ).asJava
    )

    // 表的属性
    val options: CreateTableOptions = new CreateTableOptions()
      .setNumReplicas(1)
      .addHashPartitions(List("name").asJava, 6)

    // 2. 创建表
    client.createTable(name, schema, options)
  }

  @Test
  def insertRow(): Unit = {
    // 1. 打开表
    val table = client.openTable("simple3")
    // 2. 创建插入对象
    val insert = table.newInsert()
    // 3. 通过插入对象插入行
    val row: PartialRow = insert.getRow
    row.addString("name", "zhangsan")
    row.addInt("age", 10)
    row.addDouble("gpa", 4.5)
    // 4. 让改变生效, 提交会话
    client.newSession().apply(insert)
  }

  @Test
  def scanRows(): Unit = {
    // 1. 获取表
    val table = client.openTable("simple3")

    // 2. 创建动作
    // * 一个非常重要的优化手段, 就是在查询的时候不要把所有的行都查询出来
    // * 投影操作就是 SQL 中的选择列 select name, age...
    import scala.collection.JavaConverters._

    val scanner = client.newScannerBuilder(table)
      .setProjectedColumnNames(List("name", "age", "gpa").asJava)
      .build()

    // 3. 获取结果
    while (scanner.hasMoreRows) {
      // 一次扫描可不是只获取一行数据, 而是一批数据
      val rows = scanner.nextRows()

      while (rows.hasNext) {
        val row = rows.next()

        println(row.getString("name"), row.getInt("age"), row.getDouble("gpa"))
      }
    }
  }
}
