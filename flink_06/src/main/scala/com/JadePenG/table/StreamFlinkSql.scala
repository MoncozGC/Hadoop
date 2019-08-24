package com.JadePenG.table

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row

import scala.util.Random

/**
  * 流式处理sql
  *
  * 使用Flink SQL来统计5秒内 用户的 订单总数、订单的最大金额、订单的最小金额。
  *
  * @author Peng
  */
object StreamFlinkSql {

  //4. 创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(id: String, userId: String, money: Long, createTime: Long)

  def main(args: Array[String]): Unit = {
    /**
      * 1. 获取流处理运行环境
      * 2. 获取Table运行环境
      * 3. 设置处理时间为 EventTime
      * 4. 创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
      * 5. 创建一个自定义数据源
      * 使用for循环生成1000个订单
      * 随机生成订单ID（UUID）
      * 随机生成用户ID（0-2）
      * 随机生成订单金额（0-100）
      * 时间戳为当前系统时间
      * 每隔1秒生成一个订单
      * 6. 添加水印，允许延迟2秒
      * 7. 导入 import org.apache.flink.table.api.scala._ 隐式参数
      * 8. 使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
      * 9. 编写SQL语句统计用户订单总数、最大金额、最小金额
      * 分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
      * 10. 使用 tableEnv.sqlQuery 执行sql语句
      * 11. 将SQL的执行结果转换成DataStream再打印出来
      * 12. 启动流处理程序
      */
    //1. 获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 获取Table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //3. 设置处理时间为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //5. 创建一个自定义数据源
    val orderDataStream = env.addSource(new RichSourceFunction[Order] {
      //数据是否还执行
      var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        // - 随机生成订单ID（UUID）
        // - 随机生成用户ID（0-2）
        // - 随机生成订单金额（0-100）
        // - 时间戳为当前系统时间
        // - 每隔1秒生成一个订单
        for (i <- 0 until 1000; if (isRunning)) {
          //生成一个订单数据（订单id，用户id，订单金额， 时间戳）
          val order = Order(UUID.randomUUID().toString, Random.nextInt(3).toString, Random.nextInt(101), System.currentTimeMillis())
          //收集返回
          ctx.collect(order)
          //延迟1s
          TimeUnit.SECONDS.sleep(1)
        }
      }

      //取消操作的时候执行，程序宕机或者kill的时候执行
      override def cancel(): Unit = {
        isRunning = false
      }
    })
    //6. 添加水印
    val waterMarkDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Order] {
      //最大允许2秒钟的延迟
      var delayTime = 2000L
      //当前时间(事件时间)
      var currentTimestamp: Long = _

      /**
        * 生成水印数据, 每隔一定时间执行一次
        *
        * @return
        */
      override def getCurrentWatermark: Watermark = {
        //事件时间减去延迟时间, 标识window窗口允许延迟的事件(窗口执行事件应该是按照水印时间执行)
        //10:05:10 -2s   ->10:05:08
        val watermark = new Watermark(currentTimestamp - delayTime)

        //设置日期打印时间格式
        val format = FastDateFormat.getInstance("HH:mm:ss")

        println(s"水印时间：${format.format(watermark.getTimestamp)}, 事件时间：${format.format(currentTimestamp)}, 系统事件：${format.format(System.currentTimeMillis())}")
        watermark
      }

      /**
        * 抽取时间戳, 再日志文件中抽取时间戳
        *
        * @param element
        * @param previousElementTimestamp
        * @return
        */
      override def extractTimestamp(element: Order, previousElementTimestamp: Long): Long = {
        //获取到的数据Order
        //Order(1, "zhangsan", "2019-10-17 10:23:23", 12)
        //Order(2, "lis",      "2019-10-17 10:23:25", 12)
        //Order(3, "wangwu",   "2019-10-17 10:23:20", 12)
        //Order(4, "zhaoliu", "2019-10-17 10:23:27", 12)
        var timestamp = element.createTime
        println(element.toString)
        //获取一下, 上次执行之间和当前最新获取的时间的最大事件
        currentTimestamp = Math.max(currentTimestamp, timestamp)
        currentTimestamp
      }
    })

    //8. 使用 registerDataStream 注册表，并分别指定字段，还要指定rowTime字段
    import org.apache.flink.table.api.scala._
    tableEnv.registerDataStream("t_order", waterMarkDataStream, 'id, 'userId, 'money, 'createTime.rowtime)

    //9. 编写sql语句统计用户订单总数, 最大金额, 最小金额
    //   分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
    val sql =
    """
      |select
      | userId,
      | count(1) as totalCount,
      | max(money) as maxMoney,
      | min(money) as minMoney
      | from t_order
      | group by
      |   tumble(createTime, interval '5' second),
      |   userId
      """.stripMargin

    //执行查询
    val table: Table = tableEnv.sqlQuery(sql)

    //转换成DStream打印
    tableEnv.toAppendStream[Row](table).print()

    //启动流处理程序
    env.execute("StreamSql")
  }
}