package com.JadePeng.scala.step02

/**
  * 定义一个抽象类，用于继承
  */
abstract class Animal {
  //定义一个抽象字段
  var age:Int
  //非抽象字段
  val weight:Double = 35f

  //抽象方法，没有具体的实现
  def color()

  //非抽象方法，有具体的实现
  def eat()= {
    println("吃东西")
  }

  //使用final关键字进行定义，表示不能被重写
  final def action() ={
    println("跳跃/爬树")
  }
}

/**
  * 定义一个猴子类
  * 如果要实现父类的方法: Ctrl+I
  * 如果要重写父类的方法：Ctrl+O
  */
class Monkey extends Animal{
  //重写父类的字段（父类没有具体实现），可以选择性的使用override
  override var age: Int = _

  //非抽象字段，父类有具体的实现，一定要加override
  override val weight: Double = 40

  //非抽象方法没有具体的实现，重写父类方法，必须要使用override
  override def eat(): Unit = super.eat()

  //抽象方法，没有实现（重写父类抽象方法，可以选择性使用override）
  override def color(): Unit = {
    println("棕色")
  }
}

//伴生对象
object Monkey{
  def main(args: Array[String]): Unit = {
    val monkey = new Monkey
    monkey.color()
    println(monkey.age)
    println(monkey.weight)
  }
}
