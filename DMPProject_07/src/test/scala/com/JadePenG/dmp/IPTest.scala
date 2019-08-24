package com.JadePenG.dmp

import com.maxmind.geoip.{Location, LookupService}
import org.junit.Test
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  *
  * @author Peng
  */
class IPTest {

  /**
    * 使用ip2region
    * 根据ip获取省市信息
    */
  @Test
  def ip2Region(): Unit = {
    val ip = "14.131.228.195"
    //1. 创建入口
    val searcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")

    //2. 进行搜索
    val region = searcher.btreeSearch(ip).getRegion

    //3. 抽取内容
    val regions: Array[String] = region.split("\\|")
    println(regions(2), regions(3))
  }

  /**
    * GeoLite类库(获取到的省市是拼音形式的)
    */
  @Test
  def ip2Location(): Unit = {
    val ip = "14.131.228.195"
    //1. 创建入口   获取数据  缓存在哪
    //最好一个节点对应一个LookupService
    val lookup: LookupService = new LookupService("dataset/GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
    //2. 进行搜索
    val location: Location = lookup.getLocation(ip)
    //3. 获取结果
    // latitude纬度   longitude经度
    println(location.region, location.city, location.latitude, location.longitude)

  }
}
