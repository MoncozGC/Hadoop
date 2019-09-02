package com.JadePeng.scala.step03

class MyGirl(val name:String, val fv:Int) extends Ordered[MyGirl]{
  override def compare(that: MyGirl): Int = {
    -(this.fv-that.fv)
  }

  override def toString = s"MyGirl($name, $fv)"
}

object MyGirl {
  def main(args: Array[String]): Unit = {
    val arr = Array[MyGirl](new MyGirl("canglaoshi", 89), new MyGirl("boduolaoshi", 92), new MyGirl("songlaoshi", 90))

    val sorted: Array[MyGirl] = arr.sortBy(x=>x)
    println(sorted.toBuffer)
  }
}
