package com.JadePenG.report

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 对于所有的 Report 来说, 整体流程是一样的
  * 取出来流程, 放在一个单独的类中
  * 针对每个报表不同的情况, 再编写processor 表示特点
  */
object DailyReportRunner {

  def main(args: Array[String]): Unit = {
    import com.JadePenG.utils.ConfigHelper._
    import com.JadePenG.utils.KuduHelper._

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("region_report")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    // 在此处注册所有的报表统计对象
    val processorList = List[ReportProcessor](
      RegionReportProcessor,
      AdsRegionReport
    )

    for (processor <- processorList) {
      // 2. 读取数据
      val source: Option[DataFrame] = spark.readKuduTable(processor.sourceTableName())

      if (source.isDefined) {
        // 3. 数据处理
        val result = processor.process(source.get)

        // 4. 数据落地
        spark.createKuduTable(processor.targetTableName(), processor.targetSchema(), processor.keys())
        result.writeToKudu(processor.targetTableName())
      }
    }
  }
}
