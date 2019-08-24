package com.JadePenG.report

import com.JadePenG.etl.ETLRunner
import com.JadePenG.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.DataFrame

object RegionReportProcessor extends ReportProcessor {

  override def sourceTableName(): String = {
    ETLRunner.ODS_TABLE
  }

  override def process(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    dataFrame.groupBy('region, 'city)
      .agg(count("*") as "count")
      .select('region, 'city, 'count)
  }

  override def targetTableName(): String = {
    "report_region_" + KuduHelper.FORMAT_DATE()
  }

  override def targetSchema(): Schema = {
    import scala.collection.JavaConverters._
    new Schema(
      List(
        new ColumnSchemaBuilder("region", Type.STRING).key(true).nullable(false).build(),
        new ColumnSchemaBuilder("city", Type.STRING).key(true).nullable(false).build(),
        new ColumnSchemaBuilder("count", Type.INT64).key(false).nullable(true).build()
      ).asJava
    )
  }

  override def keys(): Seq[String] = {
    Seq("region", "city")
  }
}
