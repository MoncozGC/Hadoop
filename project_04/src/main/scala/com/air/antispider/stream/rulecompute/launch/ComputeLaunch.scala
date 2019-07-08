package com.air.antispider.stream.rulecompute.launch

import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实时计算任务的驱动入口
  * 功能：读取kafka预处理后的数据、八项指标分析、打分、离线数据存储（HDFS）
  */
object ComputeLaunch {

  /**
    * 驱动入口函数，只做初始化操作，不实现具体的业务逻辑
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //当应用被停止的时候，进行如下设置可以保证当前批次执行完之后再停止应用。
    System.setProperty("spark.streaming.stopGracefullyOnShutdown", "true")

    //创建sparkConf
    val conf = new SparkConf().setAppName("streaming-rulecompute").setMaster("local[2]")

    //创建sparkcontext
    val sc: SparkContext = new SparkContext(conf)

    //创建sqlcontext
    //val spark = SparkSession(sc)
    val sqlContext = new SQLContext(sc)

    //构建kafka的配置参数
    val brokerList = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList
    )

    //指定消费者主题
    val sourceTopic = Set(PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties"))

    //加载zk的配置信息（因为我们需要将offset维护到zk上）
    val zkHosts = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    //指定offset写入的位置
    val zkPath = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")

    //创建zk的客户端实例
    val zkClient = new ZkClient(zkHosts, 30000, 30000)

    //创建setupSsc方法，在这个方法中实现所有的业务逻辑
    val ssc = setupSsc(sc, sqlContext, kafkaParams, sourceTopic, zkClient, zkHosts, zkPath)

    ///启动任务
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 实现具体的业务逻辑
    *
    * @param sc
    * @param sqlContext
    * @param kafkaParams
    * @param sourceTopic
    * @param zkClient
    * @param zkHosts
    * @param zkPath
    * @return
    */
  def setupSsc(sc: SparkContext, sqlContext: SQLContext, kafkaParams: Map[String, String], sourceTopic: Set[String], zkClient: ZkClient, zkHosts: String, zkPath: String) = {
    //创建streamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))

    //消费kafka的数据
    val lines: InputDStream[(String, String)] = createConsumerDirectStream(ssc, kafkaParams, sourceTopic, zkClient, zkHosts, zkPath)

    //打印测试
    lines.foreachRDD(rdd => {
      //计算数据（结果）
      rdd.foreach(println(_))
    })

    //保存偏移量
    lines.foreachRDD(rdd =>
      KafkaOffsetUtil.saveOffsets(zkClient, zkHosts, zkPath, rdd)
    )
    ssc
  }

  /**
    * 通过direct方式消费kafka中的数据，然后自己维护偏移量
    *
    * @param ssc
    * @param kafkaParams
    * @param sourceTopic
    * @param zkClient
    * @param zkHosts
    * @param zkPath
    * @return
    */
  def createConsumerDirectStream(ssc: StreamingContext, kafkaParams: Map[String, String], sourceTopic: Set[String], zkClient: ZkClient, zkHosts: String, zkPath: String) = {

    /**
      * 代码思路：
      * 1：读取offset
      * 2：拿到offset以后，指定offset位置进行消费
      * 3：拿不到就直接消费
      */
    val topic = sourceTopic.last
    //1：读取offset
    val offsets: Option[Map[TopicAndPartition, Long]] = KafkaOffsetUtil.readOffsets(zkClient, zkHosts, zkPath, topic)
    //拿到offset以后，需要考虑两种情况
    val kafkaDStream: InputDStream[(String, String)] = offsets match {
      //如果没有偏移量数据，直接消费kafka的数据
      case None => {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, sourceTopic)
      }
      //如果由偏移量数据，根据上次消费的位置继续消费
      case Some(fromOffset) => {
        val messageHeader = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, messageHeader)
      }
    }
    kafkaDStream
  }
}
