package com.air.antispider.stream.dataprocess.launch

import com.air.antispider.stream.common.bean._
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.dataprocess.businessprocess._
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, TravelTypeEnum}
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable
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
    //每次拉取的数据，1000*分区数*采样时间=拉取数据量
    System.setProperty("spark.streaming.kafka.maxRatePerPartition", "1000")

    //创建sparkConf对象
    val conf: SparkConf = new SparkConf().setAppName("dataprocess-streaming").setMaster("local[2]")
      .set("spark.metrics.conf.executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")

    //创建sparkContext对象
    val sc: SparkContext = new SparkContext(conf)

    //读取kafka的集群地址
    val brokers: String = PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties")
    val kafkaParams: Map[String, String] = Map(
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
    * 26/Jun/2019:03:42:54 -0800#CS#POST /B2C40/query/jaxb/direct/query.ao HTTP/1.1#CS#POST#CS#application/x-www-form-urlencoded; charset=UTF-8#CS#json=%7B%22depcity%22%3A%22CAN%22%2C+%22arrcity%22%3A%22WUH%22%2C+%22flightdate%22%3A%2220180220%22%2C+%22adultnum%22%3A%221%22%2C+%22childnum%22%3A%220%22%2C+%22infantnum%22%3A%220%22%2C+%22cabinorder%22%3A%220%22%2C+%22airline%22%3A%221%22%2C+%22flytype%22%3A%220%22%2C+%22international%22%3A%220%22%2C+%22action%22%3A%220%22%2C+%22segtype%22%3A%221%22%2C+%22cache%22%3A%220%22%2C+%22preUrl%22%3A%22%22%2C+%22isMember%22%3A%22%22%7D#CS#http://b2c.csair.com/B2C40/modules/bookingnew/main/flightSelectDirect.html?t=S&c1=CAN&c2=WUH&d1=2019-07-02&at=1&ct=0&it=0#CS#192.168.180.1#CS#Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36#CS#2019-06-26T03:42:54-08:00#CS#192.168.180.111#CS#JSESSIONID=782121159357B98CA6112554CF44321E; sid=b5cc11e02e154ac5b0f3609332f86803; aid=8ae8768760927e280160bb348bef3e12; identifyStatus=N; userType4logCookie=M; userId4logCookie=13818791413; useridCookie=13818791413; userCodeCookie=13818791413; temp_zh=cou%3D0%3Bsegt%3D%E5%8D%95%E7%A8%8B%3Btime%3D2018-01-13%3B%E5%B9%BF%E5%B7%9E-%E5%8C%97%E4%BA%AC%3B1%2C0%2C0%3B%26cou%3D1%3Bsegt%3D%E5%8D%95%E7%A8%8B%3Btime%3D2019-07-02%3B%E5%B9%BF%E5%B7%9E-%E6%88%90%E9%83%BD%3B1%2C0%2C0%3B%26; JSESSIONID=782121159357B98CA6112554CF44321E; WT-FPC=id=211.103.142.26-608782688.30635197:lv=1516170718655:ss=1516170709449:fs=1513243317440:pn=2:vn=10; language=zh_CN; WT.al_flight=WT.al_hctype(S)%3AWT.al_adultnum(1)%3AWT.al_childnum(0)%3AWT.al_infantnum(0)%3AWT.al_orgcity1(CAN)%3AWT.al_dstcity1(CTU)%3AWT.al_orgdate1(2019-07-02)
    * 26/Jun/2019:03:42:54 -0800#CS#POST /B2C40/dist/main/images/common.png HTTP/1.1#CS#POST#CS#application/x-www-form-urlencoded; charset=UTF-8#CS#json=%7B%22depcity%22%3A%22CAN%22%2C+%22arrcity%22%3A%22WUH%22%2C+%22flightdate%22%3A%2220180220%22%2C+%22adultnum%22%3A%221%22%2C+%22childnum%22%3A%220%22%2C+%22infantnum%22%3A%220%22%2C+%22cabinorder%22%3A%220%22%2C+%22airline%22%3A%221%22%2C+%22flytype%22%3A%220%22%2C+%22international%22%3A%220%22%2C+%22action%22%3A%220%22%2C+%22segtype%22%3A%221%22%2C+%22cache%22%3A%220%22%2C+%22preUrl%22%3A%22%22%2C+%22isMember%22%3A%22%22%7D#CS#http://b2c.csair.com/B2C40/modules/bookingnew/main/flightSelectDirect.html?t=S&c1=CAN&c2=WUH&d1=2018-02-20&at=1&ct=0&it=0#CS#192.168.180.1#CS#Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36#CS#2019-06-26T03:42:54-08:00#CS#192.168.180.111#CS#JSESSIONID=782121159357B98CA6112554CF44321E; sid=b5cc11e02e154ac5b0f3609332f86803; aid=8ae8768760927e280160bb348bef3e12; identifyStatus=N; userType4logCookie=M; userId4logCookie=13818791413; useridCookie=13818791413; userCodeCookie=13818791413; temp_zh=cou%3D0%3Bsegt%3D%E5%8D%95%E7%A8%8B%3Btime%3D2018-01-13%3B%E5%B9%BF%E5%B7%9E-%E5%8C%97%E4%BA%AC%3B1%2C0%2C0%3B%26cou%3D1%3Bsegt%3D%E5%8D%95%E7%A8%8B%3Btime%3D2018-01-17%3B%E5%B9%BF%E5%B7%9E-%E6%88%90%E9%83%BD%3B1%2C0%2C0%3B%26; JSESSIONID=782121159357B98CA6112554CF44321E; WT-FPC=id=211.103.142.26-608782688.30635197:lv=1516170718655:ss=1516170709449:fs=1513243317440:pn=2:vn=10; language=zh_CN; WT.al_flight=WT.al_hctype(S)%3AWT.al_adultnum(1)%3AWT.al_childnum(0)%3AWT.al_infantnum(0)%3AWT.al_orgcity1(CAN)%3AWT.al_dstcity1(CTU)%3AWT.al_orgdate1(2018-01-17)
    *
    * @param sc          sparkContext对象
    * @param kafkaParams kafka的集群地址
    * @param topic       指定消费的topic
    */
  def setupSsc(sc: SparkContext, kafkaParams: Map[String, String], topic: Set[String]): StreamingContext = {
    //创建streamingContext对象
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

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
    @volatile var filterRuleRef: Broadcast[ArrayBuffer[String]] = sc.broadcast(filterRuleList)

    /**
      * 数据分类的思路:
      * 1. 读取数据库配置的分类规则信息
      * 2. 将读取的分类规则信息进行广播到executor节点
      * 3. 监视广播变量是否发生改变, 如果一旦发生了改变就需要重新广播
      * 4. 对rdd数据的请求地址进行正则表达式匹配. 如果匹配上了就打标签
      */
    val ruleMapList: mutable.HashMap[String, ArrayBuffer[String]] = AnalyzerRuleDB.queryClassifyRule()
    //将读取到的规则信息进行广播
    //这个关键字表示线程安全，会被多个线程所使用
    @volatile var ruleMapRef: Broadcast[mutable.HashMap[String, ArrayBuffer[String]]] = sc.broadcast(ruleMapList)

    /**
      * 数据解析的思路:
      * 1. 读取数据库配置的数据解析规则信息
      * 2. 将读取到的数据解析规则广播到executor节点
      * 3. 监视广播变量是否发生了改变, 如果一旦发生了改变, 那么需要重新广播
      * 4. 对rdd的数据的请求地址进行正则表达式的匹配, 如果匹配上了进行数据的解析
      */
    //查询规则数据
    val queryRuleList: List[AnalyzeRule] = AnalyzerRuleDB.queryRule(BehaviorTypeEnum.Query.id)
    //预定规则数据
    val bookRuleList: List[AnalyzeRule] = AnalyzerRuleDB.queryRule(BehaviorTypeEnum.Book.id)
    //将查询规则和预定规则放到map对象中进行广播
    val queryAndBookMap: mutable.HashMap[String, List[AnalyzeRule]] = new mutable.HashMap[String, List[AnalyzeRule]]()
    queryAndBookMap.put("QueryRule", queryRuleList)
    queryAndBookMap.put("BookRule", bookRuleList)
    //一起广播出去
    @volatile var queryAndBookRuleRef: Broadcast[mutable.HashMap[String, List[AnalyzeRule]]] = sc.broadcast(queryAndBookMap)


    /**
      * 数据加工的思路:
      * 1. 读取mysql中配置的黑名单ip
      * 2. 将读取到的黑名单ip广播到executor节点
      * 3. 监控黑名单ip是否发生了改变, 如果改变重新广播
      * 4. 判断当前ip是否为黑名单ip, 如果是就标记为true
      */
    //查询黑名单ip
    val ipBlackList: ArrayBuffer[String] = AnalyzerRuleDB.queryIpBlackList()
    //广播到executor节点
    @volatile var ipBlackListRef: Broadcast[ArrayBuffer[String]] = sc.broadcast(ipBlackList)

    //获取jedis连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    //打印数据
    //lines.foreachRDD(rdd=>rdd.foreach(println(_)))

    //业务处理,foreachRDD：在driver端执行  DStream拿到的RDD在物理上只有一个RDD, 逻辑上有多个RDD
    lines.foreachRDD(foreachFunc = rdd => {
      //监视过滤规则广播变量是否发生了改变
      filterRuleRef = BroadcastProcess.monitorFilterRule(sc, filterRuleRef, jedis)
      //监视数据分类规则广播是否发生了改变
      ruleMapRef = BroadcastProcess.monitorClassifyRule(sc, ruleMapRef, jedis)
      //监视数据解析规则广播是否发生了改变
      queryAndBookRuleRef = BroadcastProcess.monitorQueryAndBookRule(sc, queryAndBookRuleRef, jedis)
      //监视黑名单ip广播是否发生了改变
      ipBlackListRef = BroadcastProcess.monitorIpBlackList(sc, ipBlackListRef, jedis)

      //TODO 1. 首先使用缓存对rdd进行存储，如果rdd在我们的job中多次反复的使用的话，要加上缓存，提高执行效率
      //将数据优先放到内存中, 能存就存,不能存就不存了
      rdd.cache()

      //TODO 2. 数据拆分，将字符串转换成bean对象
      val accessLogRDD: RDD[AccessLog] = DataSplit.parseAccessLog(rdd)

      //TODO 3. 链路统计 要写入redis中所以传一个redis
      val serversCountMap: collection.Map[String, Int] = BusinessProcess.linkCount(accessLogRDD, jedis)

      //TODO 4. 数据清洗（将符合过滤规则的数据清洗掉）
      val filterRDD: RDD[AccessLog] = accessLogRDD.filter(accessLog => UrlFilter.filterUrl(accessLog, filterRuleRef.value))

      //输出测试
      rdd.foreach(println(_))
      println("===================")
      //accessLogRDD.foreach(println(_))

      //数据处理
      val processedRDD: RDD[ProcessedData] = filterRDD.map(f = record => {
        //TODO 5. 数据脱敏操作(将手机号码、身份证号码使用md5算法进行加密)
        record.httpCookie = EncryptionData.encryptionPhone(record.httpCookie)
        record.httpCookie = EncryptionData.encryptionID(record.httpCookie)

        //TODO 6. 数据分类打标签(国内\国际 查询\预定) 来一条数据读取它的请求地址然后根据分类规则进行匹配, 匹配上了就打标签
        val requestTypeLabel: RequestType = RequestTypeClassify.classifyByRequest(record, ruleMapRef.value)

        //TODO 7. 单程 往返标签
        val travelTypeLabel: TravelTypeEnum.Value = TravelTypeClassify.classifyByReferer(record.httpReferer)

        //TODO 8. 数据的解析
        //  获取查询的解析规则  QueryRule是传入的key  getOrElse返回指定key相关的value值, 如果没有就返回默认值
        val queryRules: List[AnalyzeRule] = queryAndBookRuleRef.value.getOrElse("QueryRule", null)
        //  获取预定的解析规则
        val bookRules: List[AnalyzeRule] = queryAndBookRuleRef.value.getOrElse("BookRule", null)
        //解析查询数据
        val queryRequestData: Option[QueryRequestData] = AnalyzeQueryRequest.analyzeQueryRequest(
          //分类标签, 请求的方法, 请求头字段, 请求的连接, 请求参数, 单程 往返标签, 查询的解析规则
          requestTypeLabel, record.requestMethod, record.contentType,
          record.request, record.requestBody, travelTypeLabel, queryRules)
        //解析预定数据
        val bookRequestData: Option[BookRequestData] = AnalyzeBookRequest.analyzeBookRequest(
          //------bookRules: 预定的解析规则
          requestTypeLabel, record.requestMethod, record.contentType,
          record.request, record.requestBody, travelTypeLabel, bookRules)

        //TODO 9. 数据加工(黑名单ip判断)
        val highFreIp: Boolean = IpOperation.ipFreIp(record.remoteAddr, ipBlackListRef.value)

        //TODO 10. 数据格式化
        val processedData: ProcessedData = DataPackage.dataPackage("", record, highFreIp, requestTypeLabel, travelTypeLabel, queryRequestData, bookRequestData)

        //测试解析完的查询数据
        //bookRequestData
        //测试数据格式化数据
        processedData
      })
      //测试解析完的查询数据
      //processedRDD.foreach(println(_))
      //测试数据格式化数据 toKafkaString推送到kafka的数据结构化数据
      processedRDD.foreach(record => println(record.toKafkaString()))

      //TODO 11. 数据推送, 将拼接好的字符串发送到kafka中
      //将查询的数据发送到查询的topic中
      DataSend.sendQueryDataToKafka(processedRDD)
      //将预定的数据发送到预定的topic中
      DataSend.sendBookDataToKafka(processedRDD)

      //TODO 12. 监控数据流量(每处理一批次数据就请求一次监控地址获取当前批次计算的结束时间和开始时间得到计算时长)
      SparkStreamingMonitor.streamingMonitor(sc, rdd, serversCountMap, jedis)

      //TODO 释放资源
      rdd.unpersist()
    })

    //返回ssc
    ssc
  }
}
