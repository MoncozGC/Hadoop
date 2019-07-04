package com.JadePenG.spark.step04

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过使用mysql累计单词出现的次数
  *
  * @author Peng
  */
object MyNetWorkCountByMysql {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    //每5秒采集一次数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val words: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //需要将数据写入mysql，集成SparkSql，SparkSql的核心抽象是dataFrame
    words.foreachRDD(rdd => {
      val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

      import spark.implicits._
      //切割 并转换成DataFrame
      val wordsDataFrame: DataFrame = rdd.flatMap(_.split(" ")).toDF("words")
      //转换成视图

      wordsDataFrame.createOrReplaceTempView("v_words")

      val result: DataFrame = spark.sql(
        """
          |select words,count(1) total
          |from v_words
          |group by words
        """.stripMargin)

      //封装mysql的口令
      val props: Properties = new Properties()
      props.setProperty("user", config.getString("db.user"))
      props.setProperty("password", config.getString("db.password"))

      if (!result.rdd.isEmpty()) {
        result.write.mode(SaveMode.Append).jdbc(config.getString("db.url"), config.getString("db.table"), props)
      }

    })

    ssc.start()
    ssc.awaitTermination()


  }

}
