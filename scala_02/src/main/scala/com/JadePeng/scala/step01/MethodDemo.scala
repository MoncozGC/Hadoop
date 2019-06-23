package com.JadePeng.scala.step01

/**
  * Scala方法
  *
  * @description:
  * @author Peng
  */
object MethodDemo {

  //Java方法
  //  private String MethodName(String name){
  //    方法体
  //    return 返回值
  //  }
  //scala方法
  //实例一：标准格式  方法名（参数名1：参数类型，参数名2：参数类型）：返回值类型 = { 方法体 }
  def hello(first: String, second: String): String = {
    first + second
  }

  /**
    * 实例二：定义一个方法，不定义返回值
    * 如果定义的方法没有声明返回值，但是方法体有返回值，那么就会根据方法的最后一个返回类型来进行自动推导
    */
  def hello2(first: String, second: String) = {
    first + second
  }

  /**
    * 实例三：定义一个方法，不定义返回值，返回不同类型的值
    *
    * @param first
    * @param second
    * @return
    */
  def hello3(first: String, second: String) = {
    if (first == "zhangsan") println(first)
    else println(23)
  }

  /**
    * 实例四：定义一个方法，参数给定默认值，如果不传递参数，则可以使用默认值代替
    *
    * @param first
    * @param second
    * @return
    */
  def hello4(first: String = "zhangsan", second: String) = {
    println(first + "-" + second)
  }

  /**
    * 实例五：变长参数，参数的个数是不定的，类似于java中的方法...可变参数
    *
    * @param first
    */
  def hello5(first: String*) = {
    var result = ""
    for (arg <- first) {
      result = result + arg + "-"
    }
    println(result)
  }

  /**
    * 实例六：递归方法,我们可以定义一个方法，使用方法调用自己
    * 使用递归方法的话，方法的返回值必须要显示的手动指定
    *
    * @param first
    */
  def hello6(first: Int): Int = {
    if (first <= 1)
      1
    else {
      println(first)
      first * hello6(first - 1)
      //第一次   first * hello6(first-1) = 3 * hello6（2）
      //    第二次 first * first（hello6(first-1)） = 3 * （ 2* hello6（1））
      //        第三次 first * first * first（1） = 3 *  2 * 1
    }
  }


  def hello7() = 10

  def main(args: Array[String]): Unit = {
    hello3("zhangsan", "sss")
    hello4(second = "wangWu")
    hello5("张三", "李四")
    println(hello6(3))

    for (i <- (0 to 9).reverse) {
      println(i)
    }

    println(hello("hello", "world"))
  }
}
