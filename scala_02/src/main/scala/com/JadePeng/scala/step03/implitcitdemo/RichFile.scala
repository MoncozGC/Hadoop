package com.JadePeng.scala.step03.implitcitdemo

import java.io.File
import scala.io.Source

class RichFile(val file:File){
  def readContext() ={
    Source.fromFile(file, "GBK").mkString
  }
}
object RichFile {
  def main(args: Array[String]): Unit = {
    val file: File = new File("D:\\北京黑马云计算大数据就业24期\\SCALA课程\\day03\\授课资料\\files\\file.txt")

    import MyPredef._
    val context = file.readContext()
    println(context)
  }
}
