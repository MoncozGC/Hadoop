package com.JadePeng.scala.step03.implitcitdemo

class A(c:C){
  def readBook() ={
    println("A：好书...")
  }
}
class B(c:C){
  def readBook() ={
    println("B：看不懂")
  }
  def writeBook(): Unit ={
    println("B：不会写")
  }
}
class C

object  AB {
  implicit def c2A(c:C) = new A(c)
  implicit def c2B(c:C) = new B(c)

  def main(args: Array[String]): Unit = {
    //导包：
    //1：import AB._ 表示将AB下的所有的隐式转换全部导入
    //2: import AB.c2A 表示将AB下的c2A隐式转换导入
    val c = new C
    //由于a、b类中都包含了readbook方法，所以会报错
//    c.writeBook()
//    c.readBook()
  }
}
