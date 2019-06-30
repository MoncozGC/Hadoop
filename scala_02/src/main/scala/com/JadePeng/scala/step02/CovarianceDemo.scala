package com.JadePeng.scala.step02

/**
  * 协变：是scala中非常有特色的功能函数，完美解决了java的缺憾
  * 如果有 A是 B的子类，但 Card[A] 却不是 Card[B] 的子类；而 Scala 中，只要灵活使用协变与逆变，就可以解决此类 Java 泛型问题；
  */

object CovarianceDemo {

  class Person

  class Teacher extends Person

  class Student extends Teacher

  //协变
  class Card[+T]

  class CovarianceDemo {
    def enterMeet(card: Card[Teacher]) = {
      println("只有老师及老师的子类才可以参加会议")
    }
  }

  def main(args: Array[String]): Unit = {
    val cp = new Card[Person]
    val ct = new Card[Teacher]
    val cs = new Card[Student]

    val demo = new CovarianceDemo
    demo.enterMeet(ct)
  }

}
