package com.JadePenG.spark.util

import java.sql.{DriverManager, ResultSet}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable._

/**
  * 连接数据库
  *
  * @author Peng
  */
object OffsetManager {

  val config = ConfigFactory.load()
  def conn = {
    val connection = DriverManager.getConnection(config.getString("db.url"),
      config.getString("db.user"),
      config.getString("db.password"))
    connection
  }

  /**
    * 读取偏移量信息
    * @param groupid
    * @param topic
    * @return
    */
  def apply(groupid:String, topic:String)={
    val connection = conn
    val pstmt = connection.prepareStatement("select * from t_streaming_offset where groupid=? and topic=?")
    pstmt.setString(1, groupid)
    pstmt.setString(2, topic)
    val rs: ResultSet = pstmt.executeQuery()

    val offsetRange = Map[TopicPartition, Long]()
    while (rs.next()){
      offsetRange += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
    }
    rs.close()
    pstmt.close()
    connection.close()

    offsetRange
  }

  /**
    * 将偏移量维护到mysql数据库
    * @param groupid
    * @param offsetRange
    */
  def saveCurrentBacthOffset(groupid:String, offsetRange:Array[OffsetRange])={
    val connection = conn
    val pstmt = connection.prepareStatement("replace into t_streaming_offset values(?,?,?,?)")
    for (o<-offsetRange){
      pstmt.setString(1, o.topic)
      pstmt.setInt(2, o.partition)
      pstmt.setString(3, groupid)
      pstmt.setLong(4, o.untilOffset)
      pstmt.executeUpdate()
    }
    pstmt.close()
    conn.close()
  }
}
