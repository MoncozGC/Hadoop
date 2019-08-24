package com.JadePenG.etl

import com.JadePenG.utils.KuduHelper
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.{Column, SparkSession}

object ETLRunner {

  def main(args: Array[String]): Unit = {
    import com.JadePenG.utils.ConfigHelper._
    import com.JadePenG.utils.KuduHelper._

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据集
    var source = spark.read.json("DMPProject_07/dataset/pmt.json")
    //    source.show()

    // 3. 转换
    // 这一步可能会有很多转换
    // DataFrame -> DataFrame
    // 通过 Processor 来完成这种转换
    val processorList = List[Processor](
      IPProcessor
    )

    for (processor <- processorList) {
      source = processor.process(source)
    }

    // 4. 将结果存入 Kudu
    // 4.1 选择写入 Kudu 中的列
    val selectRows: Seq[Column] = Seq(
      'sessionid, 'advertisersid, 'adorderid, 'adcreativeid, 'adplatformproviderid,
      'sdkversion, 'adplatformkey, 'putinmodeltype, 'requestmode, 'adprice, 'adppprice,
      'requestdate, 'ip, 'appid, 'appname, 'uuid, 'device, 'client, 'osversion, 'density,
      'pw, 'ph, 'longitude, 'latitude, 'region, 'city, 'ispid, 'ispname, 'networkmannerid,
      'networkmannername, 'iseffective, 'isbilling, 'adspacetype, 'adspacetypename,
      'devicetype, 'processnode, 'apptype, 'district, 'paymode, 'isbid, 'bidprice, 'winprice,
      'iswin, 'cur, 'rate, 'cnywinprice, 'imei, 'mac, 'idfa, 'openudid, 'androidid,
      'rtbprovince, 'rtbcity, 'rtbdistrict, 'rtbstreet, 'storeurl, 'realip, 'isqualityapp,
      'bidfloor, 'aw, 'ah, 'imeimd5, 'macmd5, 'idfamd5, 'openudidmd5, 'androididmd5,
      'imeisha1, 'macsha1, 'idfasha1, 'openudidsha1, 'androididsha1, 'uuidunknow, 'userid,
      'reqdate, 'reqhour, 'iptype, 'initbidprice, 'adpayment, 'agentrate, 'lomarkrate,
      'adxrate, 'title, 'keywords, 'tagid, 'callbackdate, 'channelid, 'mediatype, 'email,
      'tel, 'age, 'sex
    )
    // 可变参数的意思是多个参数, 一个集合只是一个参数
    // 把集合展开即可
    // 这东西是一个语法糖, 本身不具有含义, 不要去思考去原理是啥, 只是一个语法
    // 记住使用 collection:_* 可以展开为可变参数即可
    val result = source.select(selectRows: _*)

    // 4.2 目标类名
    // 4.3 编写 Schema

    spark.createKuduTable(ODS_TABLE, schema, keys)
    result.writeToKudu(ODS_TABLE)
  }

  // ODS 只有一个表, 还是每一天一个表
  // ODS_20190101
  val ODS_TABLE: String = KuduHelper.ODS_PREFIX + KuduHelper.FORMAT_DATE

  // Keys
  val keys = Seq("uuid")

  // Schema
  import scala.collection.JavaConverters._

  lazy val schema = new Schema(List(
    new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).key(true).build,
    new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(true).key(false).build(),
    new ColumnSchemaBuilder("advertisersid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("ip", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("appid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("appname", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("device", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("client", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("osversion", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("density", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("pw", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("ph", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("longitude", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("latitude", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("region", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("city", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("ispid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("ispname", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("processnode", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("apptype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("district", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("paymode", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("isbid", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("iswin", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("cur", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("imei", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("mac", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("idfa", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("openudid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("androidid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("realip", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("aw", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("ah", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("userid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("reqdate", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("reqhour", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("iptype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(true).key(false).build,
    new ColumnSchemaBuilder("title", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("keywords", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("tagid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("channelid", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(true).key(false).build,
    new ColumnSchemaBuilder("email", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("tel", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("age", Type.STRING).nullable(true).key(false).build,
    new ColumnSchemaBuilder("sex", Type.STRING).nullable(true).key(false).build
  ).asJava)
}
