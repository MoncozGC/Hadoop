package com.JadePenG.dmp

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
  *
  * @author Peng
  */
class KuduHTest {

  @Test
  def t1(): Unit ={
    //1. 创建SparkSession
    import com.JadePenG.utils.ConfigHelper._

    val spark = SparkSession.builder()
      .getOrCreate()

    val kuduContext = new KuduContext("", spark.sparkContext)

    //spark.readKuduTable()

    //kuduContext.createTable()
  }
}
