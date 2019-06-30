package com.JadePeng.scala.step02

object TestTeacher {
  def main(args: Array[String]): Unit = {
    val teacher = new Teacher("张三", 23, "F")
    println(teacher.name)
    teacher.age = 25

    teacher.tel = "abc"
  }
}
