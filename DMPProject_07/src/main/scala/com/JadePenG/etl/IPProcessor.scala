package com.JadePenG.etl

import com.maxmind.geoip.LookupService
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * source -> uuid, ... ip
  * result -> uuid, ... ip, region, city, longitude, latitude
  * 扩充四个列
  *
  * 1. 找到思路
  *   1. 想清楚需求
  *   2. 找到结果集的样子
  * 2. 做每一步的时候, 哪怕有一些功能不会写, 没关系, 看一下代码
  */
object IPProcessor extends Processor {

  override def process(dataFrame: DataFrame): DataFrame = {
    // 算子: map or mapPartitions
    // schema 问题: 添加四个列, mapPartitions -> iter -> Row
    // row 1 (ip) -> row 2 (ip, region....)
    val newData = dataFrame.rdd.mapPartitions(convertIP)

    // row 多了四个列, 但是 schema 并没有多
    val newSchema: StructType = dataFrame.schema
      .add("region", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("longitude", DoubleType, nullable = true)
      .add("latitude", DoubleType, nullable = true)

    dataFrame.sparkSession.createDataFrame(newData, newSchema)
  }

  def convertIP(rows: Iterator[Row]): Iterator[Row] = {
    // 这种入口, 要放在 rows 的 map 外面
    val dbSearcher = new DbSearcher(new DbConfig(), "DMPProject_07/dataset/ip2region.db")
    val lookupService = new LookupService("DMPProject_07/dataset/GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)

    rows.map(row => {
      // 1. 获取 IP
      val ip = row.getAs[String]("ip")

      // 2. 通过 IP 获取 region, city
      val regions = dbSearcher.btreeSearch(ip).getRegion.split("\\|")
      val region = regions(2)
      val city = regions(3)

      // 3. 通过 IP 获取 经纬度
      val location = lookupService.getLocation(ip)
      val longitude = location.longitude.toDouble
      val latitude = location.latitude.toDouble

      // 4. 创建新的 Row 对象
      val seq = row.toSeq :+ region :+ city :+ longitude :+ latitude
      Row.fromSeq(seq)
    })
  }
}
