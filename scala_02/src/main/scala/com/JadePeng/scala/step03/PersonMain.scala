package com.JadePeng.scala.step03

@SerialVersionUID(1L)
class Person extends Serializable {
  override def toString = name + "," + age

  val name = "Nick"
  val age = 20

}

object PersonMain extends App{
  override def main(args: Array[String]): Unit = {
    import java.io.{FileOutputStream, FileInputStream, ObjectOutputStream, ObjectInputStream}

    val nick = new Person
    val out = new ObjectOutputStream(new FileOutputStream("Nick.obj"))
    out.writeObject(nick)
    out.close()

    val in = new ObjectInputStream(new FileInputStream("Nick.obj"))
    val saveNick = in.readObject()
    in.close()
    println(saveNick)

    import scala.util.matching.Regex
    val pattern1 = new Regex("(S|s)cala")
    val pattern2 = "(S|s)cala".r
    val str = "Scala is scalable and cool"
    println((pattern2.findAllIn(str)).mkString(","))
  }
}
