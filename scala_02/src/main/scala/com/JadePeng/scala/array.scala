package com.JadePeng.scala

/**
  * 在一个二维数组中（每个一维数组的长度相同），每一行都按照从左到右递增的顺序排序，
  * 每一列都按照从上到下递增的顺序排序。请完成一个函数，输入这样的一个二维数组和一个整数，判断数组中是否含有该整数。
  *
  * @author Peng
  */
object array {
  def find(target: Int, array: Array[Array[Int]]): Boolean = {
    val arrayLen = array.length
    var res = false
    for (i: Int <- 0 until arrayLen) {
      for (j: Int <- 0 until array(i).length) {
        if (target == array(i)(j)) {
          res = true
          println(array(i)(j))
        }
      }
    }
    res
  }

  def main(args: Array[String]): Unit = {
    val arr = Array(Array(1, 2, 10), Array(11, 20, 30))
    println(find(10, arr))
  }
}