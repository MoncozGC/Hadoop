package com.JadePeng.scala.step02

/**
  * 多态
  */
abstract class Element {
  def demo() = {
    println("Element demo") //1
  }
}

class ArrayElement extends Element {
  override def demo(): Unit = {
    println("ArrayElement demo") //2
  }
}

class LineElement extends ArrayElement {
  override def demo(): Unit = {
    println("LineElement demo") //3
  }
}

class UniForElement extends Element

object ElementTest {
  //参数类型是基类，任何子类实例化都需要传递
  def invoke(e: Element) = {
    e.demo()
  }

  def main(args: Array[String]): Unit = {
    invoke(new ArrayElement()) //父类引用指向子类对象
    invoke(new LineElement()) //父类引用指向子类对象
    invoke(new UniForElement()) //没有重写父类的方法，所以直接调用父类方法

    //2 3 1

  }
}
