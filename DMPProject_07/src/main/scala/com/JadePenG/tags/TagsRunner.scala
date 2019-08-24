package com.JadePenG.tags

import ch.hsr.geohash.GeoHash
import com.JadePenG.area.AreaRunnerStep02
import com.JadePenG.etl.ETLRunner
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.io.Source

/**
  * 打标签
  *
  * @author Peng
  */
object TagsRunner {
  private var deviceBC: Broadcast[Map[String, String]] = _

  def main(args: Array[String]): Unit = {
    // 1. SparkSession
    import com.JadePenG.utils.ConfigHelper._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder()
      .appName("area")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    val toHash = udf(genGeoHash _)

    // 2. 读取数据集
    var (ods, app, area, deviceDic) = readFrames(spark)
    // 2.1. join ods app
    ods = ods.join(app, ods.col("appid") === app.col("appid"), "left")
    // 2.2. join ods area
    // (ods(longitude, latitude) -> ods(geoHash)) joing area(geoHash, area) => ods(longitude, latitude, geoHash, area)
    ods = ods.withColumn("geoHash", toHash('longitude, 'latitude))
    ods = ods.join(area, ods.col("geoHash") === area.col("geoHash"), "left")

    // 1创建广播对象 2在算子中通过value使用
    deviceBC = spark.sparkContext.broadcast(deviceDic)

    // 3. 标签生成, map: ODS -> IdsAndTags(ids, tags)
    // Dataset[Row] -> Dataset[IdsAndTags]
    //+--------------------+--------------------+
    //|                 ids|                tags|
    //+--------------------+--------------------+
    //|Map(openudidsha1 ...|Map(KW美文 -> 1.0, ...|
    //|Map(openudidsha1 ...|Map(S1 -> 1.0, C合...|
    val generatedTags = ods.map(genTags)
    generatedTags.show()
    // 4. ...
  }

  /**
    * Row -> IdsAndTags
    * 参数: Row
    * 返回值: IdsAndTags
    */
  def genTags(row: Row): IdsAndTags = {
    val ids = getIds(row)
    val tags = getTags(row)
    IdsAndTags(ids, tags)
  }

  /**
    * 获取标签
    *
    * @param row 数据
    * @return 返回的标签, key = AT1, value = 1
    */
  def getTags(row: Row): Map[String, Double] = {
    val result = mutable.Map[String, Double]()

    // 广告类型标签, 指用户看到的广告类型
    val adType = "AT" + row.getAs[String]("adspacetype")
    result += (adType -> 1)

    // 渠道标签
    val channelId = "CI" + row.getAs[String]("channelid")
    result += (channelId -> 1)

    // 关键字标签
    row.getAs[String]("keywords")
      .split(",")
      .foreach(item => result += (("KW" + item) -> 1))

    // 地域标签
    val region = "R" + row.getAs[String]("region")
    val city = "C" + row.getAs[String]("city")
    result += (region -> 1) += (city -> 1)

    // 性别标签, 年龄标签
    val sex = "S" + row.getAs[String]("sex")
    val age = "A" + row.getAs[String]("age")
    result += (sex -> 1) += (age -> 1)

    // AppName 标签
    // 1. 读取 appID_name, 生成 Dataset
    // 2. ods.join(app)
    // 3. 取对应的列
    val appName = "AN" + row.getAs[String]("appName")
    result += (appName -> 1)

    // 商圈库 -> 商圈标签
    // 1. 读取商圈库
    // 2. ods join area
    // 3. 取值
    val area = row.getAs[String]("area") // 酒仙桥,大山子,望京
    // 酒仙桥,大山子,望京 -> List("酒仙桥", "大山子", "望京") -> List("AREA酒仙桥", "AREA大山子", "AREA望京")
    // -> List(("AREA酒仙桥", 1), ("AREA大山子", 1), ("AREA望京", 1)) + resultMap
    val areaTuples = area.split(",")
      .toList
      .map(item => ("AREA" + item, 1.toDouble))
    result ++= areaTuples

    // 设备标签
    // osversion(1), networkmannername(4G), ispname(联通) -> D00010001: 1, D00020001: 1, D00030001: 1
    // 1. 读取数据集
    // 2. 广播 devicedic
    // 3. 在此处进行匹配
    val deviceMap = deviceBC.value

    val os = row.getAs[String]("osversion")
    val network = row.getAs[String]("networkmannername")
    val isp = row.getAs[String]("ispname")

    val ost = deviceMap.getOrElse(os, "D00010004")
    val networkt = deviceMap.getOrElse(network, "D00020005")
    val ispt = deviceMap.getOrElse(isp, "D00030004")

    result += (ost -> 1) += (networkt -> 1) += (ispt -> 1)

    result.toMap
  }

  /**
    * 给定一行数据, 获取这行数据中所有的ids
    *
    * @param row 所有的数据
    * @return 所有的 ID
    */
  def getIds(row: Row): Map[String, String] = {
    // 1. 表示出来数据中可能有什么样的ID
    // 2. 根据ID的Key找到ID的值
    // 3. 生成Map返回
    val keyList = List(
      "imei", "imeimd5", "imeisha1", "mac", "macmd5", "macsha1", "openudid",
      "openudidmd5", "openudidsha1", "idfa", "idfamd5", "idfasha1"
    )

    keyList.map(key => (key, row.getAs[String](key)))
      .filter(kv => StringUtils.isBlank(kv._2))
      // List(k -> v, k -> v, k -> v) => Map(k -> v, k -> v)
      .toMap
  }

  /**
    * 读取各种数据集
    * 参数: spark, 返回: 多个数据集, List, ()
    */
  //noinspection SourceNotClosed
  def readFrames(spark: SparkSession): (DataFrame, DataFrame, DataFrame, Map[String, String]) = {
    import com.JadePenG.utils.KuduHelper._
    import spark.implicits._

    val odsOption = spark.readKuduTable(ETLRunner.ODS_TABLE)

    // 1. 读文件, 2. 生成数据集
    val appIdNameList: Seq[(String, String)] = Source.fromFile("DMPProject_07/dataset/appID_name")
      .getLines()
      .map(_.split("##"))
      .map(item => (item(0), item(1)))
      .toSeq
    val app = appIdNameList.toDF("appid", "appName")

    val areaOption = spark.readKuduTable(AreaRunnerStep02.AREA_TABLE)

    val deviceDic = Source.fromFile("DMPProject_07/dataset/devicedic")
      .getLines()
      .map(item => item.split("##"))
      .map(item => (item(0), item(1)))
      .toMap

    // ods 如果是空, 还需继续向下执行吗?
    // 好处1: 没有侵入的阻断外部执行
    // 好处2: 外部可以什么都不做, 直接确定数据可以获取到
    if (odsOption.isEmpty || areaOption.isEmpty) {
      throw new Exception("请先ETL")
    }

    // join ods appName

    (odsOption.get, app, areaOption.get, deviceDic)
  }

  def genGeoHash(longitude: Double, latitude: Double): String = {
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }
}

/**
  * @param ids  : ID 的名字 -> ID 的值
  * @param tags : 标签 -> 标签分数
  */
case class IdsAndTags(ids: Map[String, String], tags: Map[String, Double])
