package com.JadePeng.scala.step02

/**
  * 范型的上界
  */

//1-100
//1> 到 <100
object UpperBound {

  class Person

  class Teacher extends Person

  class Student extends Person

  class Animal

  class Dog extends Animal

  class Cat extends Animal

  //必须是person或者person的子类
  //范型的上界用 <: 表示
  class UpperBound[S <: Person] {
    def say(p: S) = {
      println(p.getClass)
    }
  }

  def main(args: Array[String]): Unit = {
    val u = new UpperBound[Person]
    val p = new Person
    val t = new Teacher
    val s = new Student
    val d = new Dog
    u.say(p)
  }
}
