package com.JadePeng.scala.step01

/**
  *var  val
  * @author Peng
  */
object VarDemo01 {
  def main(args: Array[String]): Unit = {
    //定义了一个变量，这个变量是int类型
    //private int num = 10
    var num: Int = 10  //var 全拼variable，是可变数据
    //查看num的变量类型：
    //1：按alt+回车根据提示来确定返回值类型
    //2: isInstanceof
    println(num.isInstanceOf[Int])

    //类型确定以后，就不可以修改，说明scala是强数据类型的语言

    //num = 10.2
    num = 20

    //如果声明了变量不赋值， 则报语法错误
    //var num2:String = _
    //println(num2)

    //在声明变量时。可以使用var或者val来修饰，var修饰的变量是可变的，val修饰的变量是不可变的
    val age = 20
    //age = 30 //error: val全拼是value，是不可变的数据类型
    println(age)

    //思考：scala设计者为什么要设计var和val两种修饰符
    //（1）在实际编程中，我们更多的需求是获取/创建一个对象以后，读取该对象的属性
    //  或者是修改对象的属性值， 但是我们很少去改变对象本身，这时候，我们可以使用val
    //（2）val没有线程安全问题，因此效率比较高，scala的设计者推荐我们使用val,以后的开发中能使用val就不要使用var
    //（3）变量声明时， 需要初始值
    //（4）val修饰的对象不可以被改变，var修饰的变量可以改变，但是有些场景除外，（自定义对象、数组、集合等等）


    val name: String = "zhangsan"

    /**
      * 在块表达式中，代码块的最后一条语句即返回值类型
      * Unit类似于java中的void，表示没有返回值
      * 如果明确的告知块表达式，返回值是unit， 那么即使返回值是明确的数据类型，也会忽略
      * 但是如果明确的告知块表达式，返回值是数值类型，那么一定要有返回值
      */
    val hello:Int ={
      12.5
      println("hello beijing")
      23
    }
    println(hello)

    //这里的加好表示的是方法
    println(1 + 1)
    println(1.+(1))

    val name2: String = "zhangsan"



  }

  class dog{
    val name = "旺旺"
  }
}

