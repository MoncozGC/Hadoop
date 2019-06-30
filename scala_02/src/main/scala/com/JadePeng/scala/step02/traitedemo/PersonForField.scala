package com.JadePeng.scala.step02.traitedemo

/**
  * 在特质中定义具体的字段
  */
trait PersonForField {
  //定义一个具体的字段
  val age: Int = 30

  val msg:String
}

//继承特质获取字段直接添加到子类中
class StuentForField(val name:String) extends PersonForField{
    def sayHello ={
      println(s"hi, i'm ${this.name}, my age is ${this.age}")
    }

  //重写特质中的具体（有实现）的字段
  override val age: Int = 34
  //重写特质中的抽象的字段，可以选择性的添加override
  override val msg: String = "hello"
}

object  StuentForField{
  def main(args: Array[String]): Unit = {
    val s = new StuentForField("tom")
    s.sayHello
  }
}
