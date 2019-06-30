package com.JadePeng.scala.step02

object Student {
  def main(args: Array[String]): Unit = {
    val m: Monkey = new Monkey
    val p: Person = new Person
    var a: Animal = null

    //判断两个类型是否一致
    println(m.isInstanceOf[Animal])

    //判断两个类型是否一致
    if(m.isInstanceOf[Animal]){
      //将m转换成animal
      a = m.asInstanceOf[Animal]
    }
    //isIntensof不能做精确的判断
    println(a.isInstanceOf[Monkey])

    //可以做精确的对象判断
    println(a.getClass == classOf[Monkey])
    println(a.getClass == classOf[Animal])


  }
}
