package com.JadePenG.report

import com.JadePenG.etl.ETLRunner
import com.JadePenG.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.DataFrame

object AdsRegionReport extends ReportProcessor {

  override def sourceTableName(): String = {
    ETLRunner.ODS_TABLE
  }

  override def process(dataFrame: DataFrame): DataFrame = {
    val ORIGIN_TEMP_TABLE: String = "origin_temp_tmp"
    val MID_TEMP_TABLE: String = "mid_temp_table"
    lazy val sql =
      s"""
         |SELECT t.region,
         |       t.city,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode >= 1) THEN 1
         |               ELSE 0
         |           END) AS orginal_req_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode >= 2) THEN 1
         |               ELSE 0
         |           END) AS valid_req_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 1
         |                     AND t.processnode = 3) THEN 1
         |               ELSE 0
         |           END) AS ad_req_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling =1
         |                     AND t.isbid = 1
         |                     AND t.adorderid != 0) THEN 1
         |               ELSE 0
         |           END) AS join_rtx_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling =1
         |                     AND t.iswin = 1) THEN 1
         |               ELSE 0
         |           END) AS success_rtx_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 2
         |                     AND t.iseffective = 1) THEN 1
         |               ELSE 0
         |           END) AS ad_show_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 3
         |                     AND t.iseffective = 1) THEN 1
         |               ELSE 0
         |           END) AS ad_click_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 2
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1) THEN 1
         |               ELSE 0
         |           END) AS media_show_cnt,
         |       sum(CASE
         |               WHEN (t.requestmode = 3
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1) THEN 1
         |               ELSE 0
         |           END) AS media_click_cnt,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1
         |                     AND t.iswin = 1
         |                     AND t.adorderid > 20000
         |                     AND t.adcreativeid > 200000) THEN floor(t.winprice / 1000)
         |               ELSE 0
         |           END) AS dsp_pay_money,
         |       sum(CASE
         |               WHEN (t.adplatformproviderid >= 100000
         |                     AND t.iseffective = 1
         |                     AND t.isbilling = 1
         |                     AND t.iswin = 1
         |                     AND t.adorderid > 20000
         |                     AND t.adcreativeid > 200000) THEN floor(t.adpayment / 1000)
         |               ELSE 0
         |           END) AS dsp_cost_money
         |FROM $ORIGIN_TEMP_TABLE t
         |GROUP BY t.region,
         |         t.city
    """.stripMargin

    lazy val sqlWithRate =
      s"""
         |SELECT t.*,
         |       t.success_rtx_cnt / t.join_rtx_cnt AS success_rtx_rate,
         |       t.media_click_cnt / t.ad_click_cnt AS ad_click_rate
         |FROM $MID_TEMP_TABLE t
         |WHERE t.success_rtx_cnt != 0
         |  AND t.join_rtx_cnt != 0
         |  AND t.media_click_cnt != 0
         |  AND t.ad_click_cnt != 0
    """.stripMargin

    // 1. 把数据源读取出来的数据注册临表
    dataFrame.createOrReplaceTempView(ORIGIN_TEMP_TABLE)
    // 2. 执行第一条 SQL, 得到第一条 SQL 的执行结果
    val temp = dataFrame.sparkSession.sql(sql)
    // 3. 把第一条SQL的执行结果注册为临表
    temp.createOrReplaceTempView(MID_TEMP_TABLE)
    // 4. 执行第二条 SQL 语句
    val result = dataFrame.sparkSession.sql(sqlWithRate)
    result
  }

  override def targetTableName(): String = {
    "report_ads_region_" + KuduHelper.FORMAT_DATE
  }

  override def targetSchema(): Schema = {
    import scala.collection.JavaConverters._

    new Schema(
      List(
        new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("city", Type.STRING).nullable(false).key(true).build(),
        new ColumnSchemaBuilder("orginal_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("valid_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_req_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("join_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_show_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("media_click_cnt", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_pay_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("dsp_cost_money", Type.INT64).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("success_rtx_rate", Type.DOUBLE).nullable(true).key(false).build(),
        new ColumnSchemaBuilder("ad_click_rate", Type.DOUBLE).nullable(true).key(false).build()
      ).asJava
    )
  }

  override def keys(): Seq[String] = {
    Seq("region", "city")
  }
}
