package com.air.antispider.stream.rulecompute.launch

import com.air.antispider.stream.common.bean.{FlowCollocation, ProcessedData}
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import com.air.antispider.stream.rulecompute.businessprocess.{AnalyzerRuleDB, BroadcastProcess, CoreRule, QueryDataPackage}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

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
    val conf: SparkConf = new SparkConf().setAppName("streaming-rulecompute").setMaster("local[2]")

    //创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    //创建sqlContext
    //val spark = SparkSession(sc)
    val sqlContext: SQLContext = new SQLContext(sc)

    //构建kafka的配置参数
    val brokerList: String = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    val kafkaParams: Map[String, String] = Map(
      "metadata.broker.list" -> brokerList
    )

    //指定消费者主题
    val sourceTopic: Set[String] = Set(PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties"))

    //加载zk的配置信息（因为我们需要将offset维护到zk上）
    val zkHosts: String = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    //指定offset写入的位置
    val zkPath: String = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")

    //创建zk的客户端实例
    val zkClient: ZkClient = new ZkClient(zkHosts, 30000, 30000)

    //创建setupSsc方法，在这个方法中实现所有的业务逻辑
    val ssc: StreamingContext = setupSsc(sc, sqlContext, kafkaParams, sourceTopic, zkClient, zkHosts, zkPath)

    ///启动任务
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 实现具体的业务逻辑
    *
    * @param sc          sparkContext
    * @param sqlContext  SQLContext
    * @param kafkaParams kafka服务器地址
    * @param sourceTopic 消费者的topic
    * @param zkClient    zk的客户端
    * @param zkHosts     zk的集群地址
    * @param zkPath      保存offset在zk上的路径
    * @return
    */
  def setupSsc(sc: SparkContext, sqlContext: SQLContext, kafkaParams: Map[String, String], sourceTopic: Set[String], zkClient: ZkClient, zkHosts: String, zkPath: String) = {
    //创建streamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //消费kafka的数据
    val lines: InputDStream[(String, String)] = createConsumerDirectStream(ssc, kafkaParams, sourceTopic, zkClient, zkHosts, zkPath)

    //获取kafka的数据 (分区id, 具体内容)
    val message: DStream[String] = lines.map(_._2)

    /**
      * 定义实时计算任务需要的广播规则数据
      * 1. 关键页面规则
      * 2. 黑名单规则
      * 3. 流程规则
      */
    //1. 广播关键页面规则
    val queryCritcalPageList = AnalyzerRuleDB.queryCriticalPageList()
    @volatile var queryCritcalPageListRef: Broadcast[ArrayBuffer[String]] = sc.broadcast(queryCritcalPageList)
    //2. 广播黑名单规则
    val ipBlackList = AnalyzerRuleDB.queryIpBlackList()
    @volatile var ipBlackListRef = sc.broadcast(ipBlackList)
    //3. 广播流程规则
    val flowList: ArrayBuffer[FlowCollocation] = AnalyzerRuleDB.queryFlow()
    @volatile var flowListRef: Broadcast[ArrayBuffer[FlowCollocation]] = sc.broadcast(flowList)

    //获取一个jedis连接
    val jedis = JedisConnectionUtil.getJedisCluster

    //TODO 1. 封装数据, 将kafka中的字符串封装成Bean对象
    val queryDataPackageDStream: DStream[ProcessedData] = QueryDataPackage.queryDataLoadAndPackage(message)
    //加载规则并广播监视(在Driver定义，广播到Executor)
    queryDataPackageDStream.foreachRDD(rdd => {
      //1. 监视关键页面规则
      queryCritcalPageListRef = BroadcastProcess.monitorQueryCritcalPageList(sc, queryCritcalPageListRef, jedis)
      //2. 监视黑名单规则
      ipBlackListRef = BroadcastProcess.monitorIpBlackList(sc, ipBlackListRef, jedis)
      //3. 监视流程规则
      flowListRef = BroadcastProcess.monitorFlowList(sc, flowListRef, jedis)
    })

    //TODO 2. 指标计算
    //TODO 2.1 某个ip，单位时间内ip段的访问量（前两位）
    //(192.168, 100),(192.172, 220)
    val ipBlock = CoreRule.ipBlockAccessCount(queryDataPackageDStream, Seconds(10), Seconds(10))
    //将ip段转发成map类型, 因为使用起来方便
    var ipBlockAccessCountMap = collection.Map[String, Int]()
    ipBlock.foreachRDD(rdd => {
      //收集到Driver端
      ipBlockAccessCountMap = rdd.collectAsMap()
      println(s"单位时间内ip段的访问量:${ipBlockAccessCountMap}")
    })

    //TODO 2.2 某个ip, 单位时间内ip的访问量
    val ip = CoreRule.ipAccessCount(queryDataPackageDStream, Seconds(10), Seconds(10))
    //将ip转换成map类型，因为使用起来方便
    var ipAccessCountMap = collection.Map[String, Int]()
    ip.foreachRDD(rdd => {
      //收集到Driver端
      ipAccessCountMap = rdd.collectAsMap()
      println(s"单位时间内ip的访问量:${ipAccessCountMap}")
    })

    //TODO 2.3 某个ip，单位时间内关键页面的访问量
    val critcalPages = CoreRule.criticalPageCount(queryDataPackageDStream, Seconds(10), Seconds(10), queryCritcalPageListRef.value)
    //将关键页面转换成map类型
    var criticalPageCountMap = collection.Map[String, Int]()
    critcalPages.foreachRDD(rdd => {
      //收集到Driver端
      criticalPageCountMap = rdd.collectAsMap()
      println(s"单位时间内关键页面的访问量:${criticalPageCountMap}")
    })

    //TODO 2.4 某个ip，单位时间内ua的访问种类数
    //192.168.180.111 chrome、192.168.180.111 ie、192.168.180.111 chrome  2个User-Agent种类
    val userAgentCounts: DStream[(String, Iterable[String])] = CoreRule.userAgentAccessCount(queryDataPackageDStream, Seconds(10), Seconds(10))
    //将userAgent转换成map对象
    var userAgentAccessCountMap = collection.Map[String, Int]()
    userAgentCounts.map(record => {
      //获取到ip对应的所有的useragent
      val userAgents = record._2
      //取得useragent的种类数  相当于一个ip单位时间内使用设备的种类数 toSet去重, 我们只需要取种类数的一个就好
      val count = userAgents.toSet.size
      //record._1: IP  record._2: 用户代理(User-Agent)
      (record._1, count)
    }).foreachRDD(rdd => {
      //收集到Driver端
      userAgentAccessCountMap = rdd.collectAsMap()
      println(s"单位时间内ua的访问种类数:${userAgentAccessCountMap}")
    })

    //TODO 2.7 某个ip，单位时间内查询不同行程的次数
    val flightQuery: DStream[(String, Iterable[(String, String)])] = CoreRule.flightQuery(queryDataPackageDStream, Seconds(10), Seconds(10))
    //转换成map对象
    var flightQueryMap = collection.Map[String, Int]()
    flightQuery.map(record => {
      //获取所有的始发地 目的地
      val flightQuerys = record._2
      //获取查询次数
      val count = flightQuerys.toSet.size
      (record._1, count)
    }).foreachRDD(rdd => {
      flightQueryMap = rdd.collectAsMap()
      println(s"单位时间内查询不同行程的次数:${flightQueryMap}")
    })



    //是否可以打印出来？ 不能 这相当于初始化 只会执行一次
    //并且不会打印出数据, 因为上面的任务在executor执行的还没有执行往数据, 还没计算出来 所有没数据
    println(ipBlockAccessCountMap)


    /*    //打印测试
        lines.foreachRDD(rdd => {
          //计算数据（结果）
          rdd.foreach(println(_))
        })*/

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
