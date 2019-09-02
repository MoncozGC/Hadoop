package com.JadePeng.scala.step03.implitcitdemo

class Cat {
  def miaomiaomiao(name:String): Unit ={
    println(name+", 喵喵喵...")
  }
}
class Dog{
  def  wangwangwang(name:String) ={
    println(name+", 汪汪汪...")
  }
}

object CatAndDog {
  implicit def dog2Cat(dog:Dog) = new Cat

  def main(args: Array[String]): Unit = {
    val dog = new Dog
    dog.wangwangwang("大黄狗")
    dog.miaomiaomiao("大黄狗")
  }
}
