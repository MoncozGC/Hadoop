package com.JadePeng.scala.step02

//逆变：也是scala中具有特色的功能，解决了java范型问题
object ContravariantDemo {

  class Person

  class Teacher extends Person

  class Student extends Teacher

  //逆变: 必须传递的是范型本身及其父类
  class Card[-T]

  class ContravariantDemo {
    def enterMeet(card: Card[Teacher]) = {
      println("只有老师及老师的子类才可以参加会议")
    }
  }

  def main(args: Array[String]): Unit = {
    val cp = new Card[Person]
    val ct = new Card[Teacher]
    val cs = new Card[Student]

    val demo = new ContravariantDemo
    demo.enterMeet(cp)
  }

}
