package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean.AccessLog
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess._
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 这是我们的数据预处理的驱动类
  * flume:落磁盘
  * 实现：链路统计、数据清洗、脱敏、拆分、封装、解析、历史爬虫判断、结构化、数据推送等需求
  */
object DataProcessLaunch {

  //这个方法是业务的驱动方法，不实现具体的业务逻辑
  //只做初始化操作
  def main(args: Array[String]): Unit = {
    //当应用程序停止的时候，会将当前批次的数据处理完成再停止（优雅停止）
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")
    //每次拉取1000条数据，1000*分区数*采样时间=拉取数据量
    System.setProperty("spark.streaming.kafka.maxRatePerPartition", "1000")

    //创建sparkconf对象
    val conf = new SparkConf().setAppName("dataprocess-streaming").setMaster("local[2]")
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")

    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //读取kafka的集群地址
    val brokers = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    val kafkaParams = Map(
      "metadata.broker.list" -> brokers
    )
    //指定消费的topic
    val topic: Set[String] = Set(PropertiesUtil.getStringByKey("source.nginx.topic", "kafkaConfig.properties"))

    //这个方法中实现我们具体的业务逻辑
    val ssc: StreamingContext = setupSsc(sc, kafkaParams, topic)

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 具体的业务处理类
    * 实现：链路统计、数据清洗、脱敏、拆分、封装、解析、历史爬虫判断、结构化、数据推送等需求
    *
    *
    * @param sc
    * @param kafkaParams
    * @param topic
    */
  def setupSsc(sc: SparkContext, kafkaParams: Map[String, String], topic: Set[String]) = {
    //创建streamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))

    //从kafka中消费数据
    val lines: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic).map(_._2)

    /**
      * 数据清洗的思路：
      * 1：读取数据库配置的过滤规则
      * 2：将读取到的数据库的过滤规则广播到executor节点
      * 3：监视广播变量是否发生了改变，如果一旦发生了改变，那么重新广播
      * 4：对rdd中的数据进行正则表达式的匹配，如果能够匹配上需要过滤，否则不过滤
      */
    //查询规则
    val filterRuleList: ArrayBuffer[String] = AnalyzerRuleDB.queryFilterRule()
    //将读取到的规则信息进行广播
    //这个关键字表示线程安全，会被多个线程所使用
    @volatile var filterRuleRef = sc.broadcast(filterRuleList)

    //获取jedis连接
    val jedis = JedisConnectionUtil.getJedisCluster

    //打印数据
    //lines.foreachRDD(rdd=>rdd.foreach(println(_)))

    //业务处理,foreachRDD：在driver端执行
    lines.foreachRDD(foreachFunc = rdd => {
      //监视过滤规则广播变量是否发生了改变
      filterRuleRef = BroadcastProcess.monitorFilterRule(sc, filterRuleRef, jedis)

      //TODO 1：首先使用缓存对rdd进行存储，如果rdd在我们的job中多次反复的使用的话，要加上缓存，提高执行效率
      rdd.cache()

      //TODO 2：数据拆分，将字符串转换成bean对象
      val accessLogRDD: RDD[AccessLog] = DataSplit.parseAccessLog(rdd)

      //TODO 3：链路统计 要写入redis中所以传一个redis
      BusinessProcess.linkCount(accessLogRDD, jedis)

      //TODO 4：数据清洗（将符合过滤规则的数据清洗掉）
      val filterRDD: RDD[AccessLog] = accessLogRDD.filter(accessLog => UrlFilter.filterUrl(accessLog, filterRuleRef.value))

      //输出测试
      filterRDD.foreach(println(_))
      println("===================")
      //accessLogRDD.foreach(println(_))

      //TODO 释放资源
      rdd.unpersist()
    })

    //返回ssc
    ssc
  }
}
