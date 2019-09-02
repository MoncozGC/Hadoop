package com.JadePeng.scala.step03.implitcitdemo

import java.io.File

object MyPredef {
  //将文件转换成增强文件类
  implicit def file2RichFile(file:File):RichFile ={
    new RichFile(file)
  }
}
