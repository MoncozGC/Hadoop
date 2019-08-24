package com.JadePenG.area

import com.typesafe.config.ConfigFactory
import okhttp3.{OkHttpClient, Request}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
  * 提供的工具:
  * 1. 通过 Http 访问高德, 参数: 经纬度, 返回: JSON 字符串, 高德给的
  * 2. 通过 JSON 解析工具解析 JSON 返回结果的对象表示, 参数: JSON String, 返回值: 对象
  */
object AreaUtils {
  private val config = ConfigFactory.load("common")
  private val client = new OkHttpClient()

  def getArea(longitude: Double, latitude: Double): Option[String] = {
    // 1. 拼接 URL
    val key = config.getString("amap.key")
    val baseUrl = config.getString("amap.baseUrl")
    val url = s"${baseUrl}v3/geocode/regeo?location=$longitude,$latitude&key=$key"

    // 2. 创建 Client

    // 3. 创建 Request
    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    // 所有涉及到网络请求的地方, 必须要考虑出错的问题
    // 网络不能假设其 是稳定的, 所以可能会断开
    try {
      // 4. 发送请求
      val response = client.newCall(request).execute()

      // 5. 处理结果
      if (response.isSuccessful) {
        // 5.1. 请求成功的时候, 获取结果返回结果
        Some(response.body().string())
      } else {
        // 5.2. 请求失败的时候, 输出日志, 返回空
        None
      }
    } catch {
      case e: Exception =>
        // logger.error(e, "访问网络获取位置的时候出错")
        e.printStackTrace()
        None
    }
  }

  def parseJson(json: String): AMapResponse = {
    // 1. 创建样例类, 省略, 字段对应字段
    // 2. 指定转换规则, 隐士转换
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    // 3. 执行解析
    Serialization.read[AMapResponse](json)
  }

  def main(args: Array[String]): Unit = {
    val longitude = 116.481488
    val latitude = 39.990464

    // 1. 通过 getArea 访问高德, 获取结构化的地址信息, 是一个 JSON 字符串
    // 因为可能有一系列的原因, 无法获取到结果, 比如说网络异常
    // 所以返回了一个 Option 表示有可能无值
    val jsonOption: Option[String] = getArea(longitude, latitude)

    // 2. Option[String] -> Option[AMapResponse]
    // 这样做的好处是, 可以把值的判定留到最后在做
    val responseOption = jsonOption.map(parseJson)

    // 3. 取出值打印
    if (responseOption.isDefined) {
      //      println(responseOption.get)

      // 有可能一个位置对应多个商圈, 比如说我们现在的位置, 是北京, 也是西三旗, 建材城西路, 金燕龙
      // 如果对应了多个商圈, 需要把多个商圈拼接起来
      // 如何拼接? 怎么拼接都可以, 但是要有一个规则, 到时候在读取的时候, 怎么拼就怎么拆
      // 西三旗|建材城|金燕龙 -> str.split("\\|")
      // 西三旗,建材城,金燕龙 -> str.split(",")
      val areas: Option[String] = responseOption.get.regeocode.addressComponent.businessAreas
        .map(list => {
          // list 中存放 BusinessArea(116.494356,39.971563,酒仙桥)
          // 但是我们对这个 area 中只有一个字段感兴趣, 是 name
          // 所以就可以 List[BusinessArea] -> List[String]
          // 然后再 mkString
          list.map(_.name).mkString(",")
        })

      println(areas.getOrElse(""))
    }
  }
}

case class AMapResponse(status: String, info: String, infocode: String, regeocode: ReGeoCode)

case class ReGeoCode(addressComponent: AddressComponent)

case class AddressComponent(businessAreas: Option[List[BusinessArea]])

case class BusinessArea(location: String, name: String)
