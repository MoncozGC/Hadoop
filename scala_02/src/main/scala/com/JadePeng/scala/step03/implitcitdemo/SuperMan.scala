package com.JadePeng.scala.step03.implitcitdemo

class Man(val name:String)
class SuperMan(val name:String){
  def heat = println("超人打怪兽")
}

object SuperMan {
  implicit  def  man2SuperMan(man:Man) = { new SuperMan(man.name) }

  def main(args: Array[String]): Unit = {
    val man = new Man("张三")
    man.heat
  }
}
