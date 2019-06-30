package com.JadePeng.scala.step02

/**
  * 上界 <:
  * 下届 >:
  */
object LowerBound {

  class Person

  class Teacher extends Person

  class Student extends Person

  class Animal

  class Dog extends Animal

  class Cat extends Animal

  //范型的下界用 >: 表示
  class LowerBound[S >: Animal] {
    def say(p: S) = {
      println(p.getClass)
    }
  }

  def main(args: Array[String]): Unit = {
    val l = new LowerBound[Animal]
    val p = new Person
    val a = new Animal
    val t = new Teacher
    val s = new Student
    val d = new Dog
    l.say(a)
  }
}
