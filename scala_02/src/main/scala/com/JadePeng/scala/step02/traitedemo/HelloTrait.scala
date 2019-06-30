package com.JadePeng.scala.step02.traitedemo

/**
  * 将特质作为接口使用
  */
trait HelloTrait {
  /**
    * 抽象方法
    */
  def sayHello(): Unit

  /**
    * 在特质中方法可以有实现，也可以没有实现
    */
  def sayHello2() = {
    println("hello say2")
  }
}

/**
  * 在定义一个特质
  */
trait MakeFirendsTrait{
  def makeFirends(c: Children) : Unit
}

/**
  * 如果继承使用extends关键字，如果使用多继承的话，使用with关联
  * @param name
  */
class  Children(val name:String) extends HelloTrait with MakeFirendsTrait with Serializable {
  /**
    * 抽象方法
    */
  override def sayHello(): Unit = {
    println(s"hello，${name}")
  }

  //添加好友
  override def makeFirends(c:Children): Unit = {
    println(s"hello, my name is ${name}")
  }
}
/**
  * 伴生对象
  */
object Children{
  def main(args: Array[String]): Unit = {
    val c1 = new Children("tom")
    val c2 = new Children("jim")
    c1.sayHello()
    c1.makeFirends(c2)
  }
}