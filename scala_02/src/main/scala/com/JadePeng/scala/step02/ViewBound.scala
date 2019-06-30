package com.JadePeng.scala.step02

class ComparInt(x: Int, y: Int) {
  def bigger = if (x > y) x else y
}

class ComparLong(x: Long, y: Long) {
  def bigger = if (x > y) x else y
}

class ComparComm[S <% Comparable[S]](o1: S, o2: S) {
  def bigger: S = if (o1.compareTo(o2) > 0) o1 else o2
}

object ViewBound {
  def main(args: Array[String]): Unit = {
    //第一种：直接调用指定的参数类型
    val c1: ComparInt = new ComparInt(2, 3)
    println(c1.bigger)

    val c2: ComparLong = new ComparLong(2, 3)
    println(c2.bigger)

    //第二种：采用视图界定相当于带上了眼镜，可以看的更深更远，可以把对象之间的隐式转换都能看到
    val c3 = new ComparComm[Int](2, 3)
    println(c3.bigger)
  }
}
