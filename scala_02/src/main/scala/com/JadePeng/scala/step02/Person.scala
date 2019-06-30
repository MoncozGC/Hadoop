package com.JadePeng.scala.step02

class Person {
    var name :String = _
  var age:Int = _
}

object Person{
  /**
    * apply方法作用：
    * 减少代码编写工作量，使用的时候不需要new初始化对象
    *
    * 使用场景;
    * kakfa：一行行的字符串，将字符串转换成对象，根据，将字符串进行分割，分割以后将参数依次赋值给class
    *
    * @param name
    * @return
    */
  def apply(name:String): Person = {
    val person = new Person()
    person.name = name
    person
  }

  def apply(name:String, age:Int): Person = {
    val person = new Person()
    person.name = name
    person.age = age
    person
  }
}

object PersonTest extends App {

  val person = new Person
  person.name ="tom"
  person.age = 23
  println(s"实例化调用：name:${person.name}, age:${person.age}")

  val person2 = Person("mike")
  println(s"apply调用：name:${person2.name}, age:${person2.age}")

  val person3 = Person("mike", 23)
  println(s"apply调用：name:${person3.name}, age:${person3.age}")
//  /**
//    * 程序入口
//    * @param args
//    */
//  def main(args: Array[String]): Unit = {
//    val person = new Person
//    person.name ="tom"
//    person.age = 23
//    println(s"实例化调用：name:${person.name}, age:${person.age}")
//
//    val person2 = Person("mike")
//    println(s"apply调用：name:${person2.name}, age:${person2.age}")
//
//    val person3 = Person("mike", 23)
//    println(s"apply调用：name:${person3.name}, age:${person3.age}")
//  }
}
