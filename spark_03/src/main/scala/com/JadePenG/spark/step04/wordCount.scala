package com.JadePenG.spark.step04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Peng
  */
object wordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("word")
    val ssc = new StreamingContext(conf, Seconds(2))

    val textFile: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    val splitDStream: DStream[String] = textFile.flatMap(_.split(" "))

    val mapDStream: DStream[(String, Int)] = splitDStream.map((_, 1))

    val reduceDStream: DStream[(String, Int)] = mapDStream.reduceByKey((_ + _))

    reduceDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
