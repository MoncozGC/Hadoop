package com.JadePenG.etl

import org.apache.spark.sql.DataFrame

trait Processor {

  def process(dataFrame: DataFrame): DataFrame
}
