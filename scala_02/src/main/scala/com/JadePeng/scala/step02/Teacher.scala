package com.JadePeng.scala.step02

/**
  * 创建一个类，类使用class关键字进行修饰
  * 这个类有个默认的空参的构造器（构造方法）
  *
  * 定义在类后面的构造器叫做主构造器
  * 1：如果主构造器中，传递的参数没有增加val/var，那么这个参数就没有get/set方法
  * 2：如果参数加上了val修饰符，那么会生成其get方法
  * 3：如果参数加上了var修饰符，那么会生成set/get方法
  *
  * 类的构造器的访问权限：
  *  主构造器的前面加上private[this], 表示这个主构造器只能在当前包下提供访问
  *  主构造器的前面加上private[package], 表示这个构造器只能指定包下提供访问
  */
class Teacher  private (val name:String, var age :Int) {
  //老师id
  val id:Int = 1
  var tel:String = _
  var sex:String = _

  /**
    * 在scala中，可以将代码直接写道class中，而在java中，代码必须包含在方法中
    * 其实在scala中，虽然是将代码写在class中，但是经过编译后，class中的代码都进入到了主构造方法中了
    */
  println("我是主构造器")

  /**
    * 在scala中this的方法名被称作辅助构造器
    * 每个辅助构造器，都必须以其他辅助构造器或者著构造器的调用作为第一句
    * @param name
    * @param age
    * @param sex
    */
  def this(name:String, age:Int, sex:String){
    //在辅助构造器中第一句先调用主构造器或者其他辅助构造器
    this(name, age)
    println("我是第一个辅助构造器")
    this.sex = sex
  }

  /**
    * 在scala中this的方法名被称作辅助构造器
    * 每个辅助构造器，都必须以其他辅助构造器或者著构造器的调用作为第一句
    * @param name
    * @param age
    * @param sex
    */
  def this(name:String, age:Int, sex:String, tel:String){
    //在辅助构造器中第一句先调用主构造器或者其他辅助构造器
    this(name, age, sex)
    println("我是第二个辅助构造器")
    this.tel = tel

  }
}
//object Teacher是class  Teacher 伴生对象
//class Teacher是object  Teacher 伴生类
//伴生类和伴生对象的最大特点是，可以相互访问；
//应用场景：
object Teacher{
  private val name: String = "zhangsan"
  def main(args: Array[String]): Unit = {
    val teacher = new Teacher("张三", 23)

    teacher.sex = "f"

    val teacher2 = new Teacher("张三", 23, "f")
  }
}
