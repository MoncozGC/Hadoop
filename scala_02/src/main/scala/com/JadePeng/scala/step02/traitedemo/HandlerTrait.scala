package com.JadePeng.scala.step02.traitedemo

/**
  * 特质的调用链
  */
trait HandlerTrait {
  def handler(data: String) = {
    println("last one")
  }
}

trait DataValidHandlerTrait extends HandlerTrait {
  println("DataValidHandlerTrait init....")

  override def handler(data: String): Unit = {
    println("check data:" + data)
  }
}

trait SingValidHandlerTrait extends HandlerTrait {
  override def handler(data: String): Unit = {
    println("check sing:" + data)
  }
}

class PersonForRespLine(val name: String) extends SingValidHandlerTrait with DataValidHandlerTrait {
  def sayHello = {
    println(s"hello, ${this.name}")
    this.handler(this.name)
  }
}

object PersonForRespLine {
  def main(args: Array[String]): Unit = {
    val p = new PersonForRespLine("tom")
    p.sayHello


  }
}
