package com.JadePeng.scala.step03

/**
  * 柯里化+隐式转换实现范型的自定义排序
  */
class PairAB[T] {

  /**
    * 使用柯里化的方式实现上下文界定
    * @param first
    * @param second
    * @param ord
    * @return
    */
  def choose(first:T, second:T)(implicit ord:Ordering[T]):T={
    if(ord.gt(first, second)) first else second
  }
}

object PairAB{
  class MyGirl(val name:String, val fv:Int)

  implicit val c2OrderedGirl = (girl:MyGirl) => new Ordered[MyGirl]{
    override def compare(that: MyGirl): Int = {
      println("function")
      girl.fv - that.fv
    }
//
//  implicit  object OrderingGirl extends Ordering[MyGirl] {
//    override def compare(x: MyGirl, y: MyGirl): Int = {
//      println("object")
//      x.fv - y.fv
//    }
//  }

}

  def main(args: Array[String]): Unit = {
    val ab = new PairAB[MyGirl]
    val c = ab.choose(new MyGirl("canglaoshi", 89), new MyGirl("boduolaoshi", 92))
    println(c.name)
  }

}
