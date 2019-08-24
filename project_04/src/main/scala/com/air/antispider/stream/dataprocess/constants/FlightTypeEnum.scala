package com.air.antispider.stream.dataprocess.constants

import com.air.antispider.stream.dataprocess.constants

/**
  * 标记航线类别 0-国内，1-国际，-1-其他
  */

object FlightTypeEnum extends Enumeration {
  type FlightTypeEnum = Value
  val National: constants.FlightTypeEnum.Value = Value(0)
  val International: constants.FlightTypeEnum.Value = Value(1)
  val Other: constants.FlightTypeEnum.Value = Value(-1)
}
