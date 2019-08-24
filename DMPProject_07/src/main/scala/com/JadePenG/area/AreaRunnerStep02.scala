package com.JadePenG.area

import ch.hsr.geohash.GeoHash
import com.JadePenG.etl.ETLRunner
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaRunnerStep02 {

  def main(args: Array[String]): Unit = {
    import com.JadePenG.utils.ConfigHelper._
    import com.JadePenG.utils.KuduHelper._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder()
      .appName("area")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    // 读取 ODS表
    val odsOption: Option[DataFrame] = spark.readKuduTable(ETLRunner.ODS_TABLE)
    val areaOption: Option[DataFrame] = spark.readKuduTable(AREA_TABLE)

    // 注册 UDF
    val toHash = udf(genGeoHash _)
    val toArea = udf(genArea _)
    spark.udf.register("toArea", genArea _)

    var result: DataFrame = null
    // 1. 判断
    if (odsOption.isDefined && areaOption.isDefined) {
      //     * ODS 表存在, Area 表存在, 已经初次生成过了
      // 思路: 商圈库中已经存在的 Key, 不需要再访问高德了
      // 1. 找到那些 key 不存在, ods join area
      // longitude, latitude, geoHash, area + geoHash, area = LEFT
      // 2. 访问高德

      val ods = odsOption.get.withColumn("geoHash", toHash('longitude, 'latitude))
      val area = areaOption.get

      // 现在有两个geoHash, SQL: select * from ods join area on geoHash = geoHash
      result = ods.join(area, ods.col("geoHash") === area.col("geoHash"), "left")
        .where(area.col("area").isNull)
        .select('longitude, 'latitude, ods.col("geoHash"))
        .withColumn("area", toArea('longitude, 'latitude))
        .select('geoHash, 'area)

    } else if (odsOption.isDefined && areaOption.isEmpty) {
      //     * ODS 表存在, Area 表不存在, 是初次生成商圈库
      val source = odsOption.get

      result = source.withColumn("geoHash", toHash('longitude, 'latitude))
        .groupBy('geoHash, 'longitude, 'latitude)
        .agg(first('geoHash))
        .withColumn("area", toArea('longitude, 'latitude))
        .select('geoHash, 'area)

      spark.createKuduTable(AREA_TABLE, schema, keys)
    }

    // 处理过程
    // 问题1: ODS 多久有一份呢? 每日一份
    // 问题2: ODS1 和 ODS2 中有不同的经纬度, 有不同的 geoHash
    // 问题3: AreaRunner 多久执行一次, 还是只执行一次, 每日执行
    // 问题4: 商圈表, 只有一个, 还是有多个? 一个
    // 问题5: 修改
    // 问题6: 这个算法还合适吗? 在商圈已经存在的前提下, 这个算法还合适吗?
    // 1. ODS + geoHash 2. 即使 geoHash 作为 Key 已经存在于商圈表中, 但是每次都还是会全量的访问 高德

    // 落地 Kudu
    spark.kuduContext.upsertRows(result, AREA_TABLE)
    // 不能这么玩了, 应该使用 KuduContext 来修改数据
    //    result.writeToKudu(AREA_TABLE)
  }

  def genGeoHash(longitude: Double, latitude: Double): String = {
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }

  def genArea(longitude: Double, latitude: Double): String = {
    val jsonOption: Option[String] = AreaUtils.getArea(longitude, latitude)

    val aMapOption: Option[AMapResponse] = jsonOption.map(AreaUtils.parseJson)

    val areaString: Option[String] = aMapOption.map(aMap => {
      val areas: Option[List[BusinessArea]] = aMap.regeocode.addressComponent.businessAreas
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
