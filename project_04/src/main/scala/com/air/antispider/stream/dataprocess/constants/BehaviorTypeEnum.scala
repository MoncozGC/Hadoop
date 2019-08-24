package com.air.antispider.stream.dataprocess.constants

import com.air.antispider.stream.dataprocess.constants

/**
  * 操作标记类别 0-查询，1-预定，-1-其他
  */
object BehaviorTypeEnum extends Enumeration {
  type BehaviorTypeEnum = Value
  val Query: constants.BehaviorTypeEnum.Value = Value(0, "Query")
  val Book: constants.BehaviorTypeEnum.Value = Value(1, "Book")
  val Other: constants.BehaviorTypeEnum.Value = Value(-1, "Other")

  def main(args: Array[String]): Unit = {
    print(BehaviorTypeEnum.Query)
  }
}
