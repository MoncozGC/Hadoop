package com.JadePeng.scala.step03

import java.io.PrintWriter

import scala.io.{BufferedSource, Source, StdIn}

/**
  *
  * @author Peng
  */
object FileOperate {
  def main(args: Array[String]): Unit = {
    //注意文件的编码格式，如果编码格式不对，那么读取报错
    val file: BufferedSource = Source.fromFile("H:\\~Big Data\\Employment\\03_大数据阶段\\day03_Scala\\资料\\files\\file.txt", "GBK");
    val lines: Iterator[String] = file.getLines()
    for (line <- lines) {
      println(line)
    }
    //注意关闭文件
    file.close()
  }
}
