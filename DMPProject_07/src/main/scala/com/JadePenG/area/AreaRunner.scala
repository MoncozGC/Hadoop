package com.JadePenG.area

import ch.hsr.geohash.GeoHash
import com.JadePenG.etl.ETLRunner
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * source: uuid, ip, city, region, longitude, latitude
  * target: geoHash, area
  */
object AreaRunner {

  def main(args: Array[String]): Unit = {
    import com.JadePenG.utils.ConfigHelper._
    import com.JadePenG.utils.KuduHelper._
    import org.apache.spark.sql.functions._

    // 1. SparkSession
    val spark = SparkSession.builder()
      .appName("area")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据源, ODS表
    val sourceOption: Option[DataFrame] = spark.readKuduTable(ETLRunner.ODS_TABLE)
    if (sourceOption.isEmpty) return
    val source: DataFrame = sourceOption.get

    // 3. 编写 UDF
    // 因为目标集中, 有两列需要生成
    // 生成列, 需要 UDF
    // 缺少 geoHash 和 Area, 针对每一列, 编写一个 UDF
    // udf 有两种, 第一种是在 SQL 中使用的, 还有一种是在命令式API中使用的
    // spark.udf.register(name, function)
    // 命令式 API 的 UDF 注册方式
    // 函数是针对单值进行处理的, 经纬度 -> geoHash
    // UDF 是针对列进行处理的, 经度列, 纬度列 -> geoHash 列
    // val geoHash: Column = toHash('longitude, 'latitude)
    val toHash = udf(genGeoHash _)
    val toArea = udf(genArea _)
    spark.udf.register("toArea", genArea _)

    // 4. 实现功能
    // withColumn 添加一列, df2 = df1.withColumn(column1)
    // val source1 = source.withColumn("geoHash", udf(...))
    // 会有一些条目的 geoHash 值是一样的, 为什么? 因为他们在同一个 geoHash 的范围内
    // 三千条数据 -> geoHash 分组 -> 一定少于三千组
    // 如果分组必定需要聚合
    // first(c1) 第一条c1不为null的数据
    val result = source.withColumn("geoHash", toHash('longitude, 'latitude))
      .groupBy('geoHash, 'longitude, 'latitude)
      .agg(first('geoHash))
      .withColumn("area", toArea('longitude, 'latitude))
      .select('geoHash, 'area)



    // 方式二: 使用 SQL 窗口函数
    // 假设给定一个数据集, name, 学科, 成绩 -> 每一个学科第一名成绩是多少 -> 名字, 学科, 名次, 成绩
    // 名字, 学科, 成绩 -> 每一个学科前十名的成绩是多少
    // select xk, rank() over (partition by xk order by score) as rank from t1 where rank < 10;
    // zhangsan 10, lisi 10, 王五 9
    // rank: 1, 1, 3    dense_rank: 1, 1, 2  row_number: 1, 2, 3
    // 1. geoHash
    // 2. row_number
    // 3. 1
    //    source.withColumn("geoHash", toHash('longitude, 'latitude))
    //      .withColumn("rowNumber", row_number() over Window.partitionBy('geoHash).orderBy('geoHash))
    //      .where('rowNumber === 1)
    //      .select('geoHash, expr("toArea(longitude, latitude) as area"))
    //      .show()
    //      .selectExpr("geoHash", "toArea(longitude, latitude)")

    // 5. 落地数据集
    spark.createKuduTable(AREA_TABLE, schema, keys)
    result.writeToKudu(AREA_TABLE)
  }

  /**
    * geoHash 的作用有两点:
    * 1. 使用 geoHash 作为商圈库的 Key
    * 2. 使用 geoHash 的话, 天然就是一个范围, 在同一范围内的坐标点, 其 geoHash 是一样的
    */
  def genGeoHash(longitude: Double, latitude: Double): String = {
    // 第三个参数 : 精度
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }

  /**
    * 根据传入的 经纬度 访问高德 API 获取商圈库信息
    * 后返回商圈库拼接起来的字符串
    */
  def genArea(longitude: Double, latitude: Double): String = {
    // 1. 访问 高德 API 获取 JSON 字符串
    // 访问 高德 API, 获取 JSON 字符串, 也可能获取不到, 所以返回 Option
    val jsonOption: Option[String] = AreaUtils.getArea(longitude, latitude)

    // 2. 解析字符串生成对象
    // Option[String] -> AreaUtils.parseJson() -> Option[AMapResponse]
    val aMapOption: Option[AMapResponse] = jsonOption.map(AreaUtils.parseJson)

    // 3. 把商圈找出来, 拼起来, 返回拼接结果
    val areaString: Option[String] = aMapOption.map(aMap => {
      // 通过对象间的关系找到商圈列表
      val areas: Option[List[BusinessArea]] = aMap.regeocode.addressComponent.businessAreas
      // 如果有值, 则拼接每一个商圈的 name, 如果无值, 返回空字符串
      if (areas.isDefined) {
        areas.get.map(_.name).mkString(",")
      } else {
        ""
      }
    })

    areaString.getOrElse("")
  }

  val AREA_TABLE: String = "B_AREA"

  import scala.collection.JavaConverters._

  val schema = new Schema(
    List(
      new ColumnSchemaBuilder("geoHash", Type.STRING).key(true).nullable(false).build(),
      new ColumnSchemaBuilder("area", Type.STRING).key(false).nullable(true).build()
    ).asJava
  )

  val keys = List("geoHash")
}
