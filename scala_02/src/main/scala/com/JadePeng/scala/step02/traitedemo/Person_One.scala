package com.JadePeng.scala.step02.traitedemo

/**
  * 在Scala中，trait也是有构造代码的，即在trait中，不包含在任何方法中的代码；
  * 继承了trait的子类，其构造机制如下：
  * 父类的构造函数先执行， class 类必须放在最左边；多个trait从左向右依次执行；构造trait时，先构造父 trait，
  * 如果多个trait继承同一个父trait，则父trait只会构造一次；所有trait构造完毕之后，子类的构造函数最后执行。
  */
class Person_One {
  println("Person's constructor!")
}
trait Logger_One {
  println("Logger's constructor!")
}
trait MyLogger_One extends Logger_One {
  println("MyLogger's constructor!")
}
trait TimeLogger_One extends Logger_One {
  println("TimeLogger's contructor!")
}

/**
  * 执行顺序，从左到右，从上到下
  */
class Student_One extends Person_One with MyLogger_One with TimeLogger_One {
  println("Student's constructor!")
}
object exe_one {
  def main(args: Array[String]): Unit = {
    val student = new Student_One
    //执行结果为：
    //      Person's constructor!
    //      Logger's constructor!
    //      MyLogger's constructor!
    //      TimeLogger's contructor!
    //      Student's constructor!
  }
}
