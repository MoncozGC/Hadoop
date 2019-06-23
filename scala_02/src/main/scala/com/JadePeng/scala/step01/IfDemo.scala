package com.JadePeng.scala.step01

/**
  * If
  *
  * @author Peng
  */
object IfDemo {
  def main(args: Array[String]): Unit = {
    //1. 在scala中不需要三目运算符
    val num = 10
    if (num > 10) println("10") else println("<=10")

    //java中的三目运算符 int a = num >10 ? 1 : 0
    val a: Int = if (num > 10) 1 else 0
    println(a)

    //2. 如果返回值是不同类型，那么返回类型必须是两者类型的超类，否则会有语法错误
    //Any是scala中的超类 相当于Java中的Object
    val b: Any = if (num > 18) "成年人" else 1
    println(b)

    //3. 如果else没有返回值，那应该返回类型：Unit，表示返回空
    val c = if (num > 18) "成年人"
    println(c)

    val d = if (num > 18) "成年人" else ()
    println(d)

    //4. if语句的返回值类型取决于最后一条语句,语句后面的分号不是必须的
    val e:Any = if (num > 18) "成年人" else {
      "小孩";
      16
    }
    println(e)

    //5. if else if
    val num2 = 2
    val f: Int = if (num2 == 0) 0 else if (num2 == 1) 1 else 2
    println(f)
  }
}
