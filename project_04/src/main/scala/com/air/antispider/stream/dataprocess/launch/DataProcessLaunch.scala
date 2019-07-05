package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess.{BusinessProcess, DataSplit}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 这是我们的数据预处理的驱动类
  * 实现：链路统计、数据清洗、脱敏、拆分、封装、解析、历史爬虫判断、结构化、数据推送等需求
  */
object DataProcessLaunch {


  //这个方法是业务的驱动方法，不实现具体的业务逻辑
  //只做初始化操作
  def main(args: Array[String]): Unit = {
    //当应用程序停止的是否,会将当前批次的数据处理完成再停止(优雅的停止)
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //每次拉取1000条数据 1000 * 分区数 * 采样时间 = 拉取数据量
    System.setProperty("spark.streaming.kafka.maxRatePerPartition", "1000")
    //创建SparkConf对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("dataprocess-streaming").set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
    //创建SparkStreaming对象
    val sc = new SparkContext(conf)

    //读取kafka的集群地址
    val brokers = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")

    val kafkaParams = Map("metadata.broker.list" -> brokers)

    //指定消费的topic
    val topic = Set(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties"))


    //这个方法中实现具体的业务逻辑
    val ssc = setupSsc(sc, kafkaParams, topic)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 具体的业务处理类
    * 实现：链路统计、数据清洗、脱敏、拆分、封装、解析、历史爬虫判断、结构化、数据推送等需求
    *
    * @param sc
    * @param kafkaParams
    * @param topic
    */
  def setupSsc(sc: SparkContext, kafkaParams: Map[String, String], topic: Set[String]) = {
    //创建StreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))


    //获取jedis连接
    val jedis = JedisConnectionUtil.getJedisCluster

    //从kafka中消费数据
    //: InputDStream[(String, String)] 第一个参数是分区号,第二个参数是内容 --map(_._2)-->  : DStream[String] 主要获取内容
    val lines: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic).map(_._2)

    lines.foreachRDD(foreachFunc = rdd => {

      //TODO:1. 首先使用缓存对rdd进行存储，如果rdd在我们的job中多次反复的使用的话，要加上缓存，提高执行效率
      rdd.cache()
      //TODO 2：数据拆分，将字符串转换成bean对象
      val accessLogRDD: RDD[AccessLog] = DataSplit.parseAccessLog(rdd)

      //TODO 3:链路统计 要写入redis中所以传一个redis
      BusinessProcess.linkCount(rdd, jedis)

      accessLogRDD.foreach(println(_))

      //释放资源
      rdd.unpersist()
    })
    ssc
  }
}
