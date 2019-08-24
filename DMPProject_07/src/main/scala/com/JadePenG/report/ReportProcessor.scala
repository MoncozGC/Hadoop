package com.JadePenG.report

import org.apache.kudu.Schema
import org.apache.spark.sql.DataFrame

/**
  * 地域统计叫做 RegionProcessor
  * 广告统计叫做 AdRegionProcessor
  */
trait ReportProcessor {

  //提供源表的名称
  def sourceTableName(): String

  //提供数据处理的过程
  def process(dataFrame: DataFrame): DataFrame

  //提供目标表的名称
  def targetTableName(): String

  //提供目标表的结构信息
  def targetSchema(): Schema

  //提供目标表的分区Key
  def keys(): Seq[String]
}
