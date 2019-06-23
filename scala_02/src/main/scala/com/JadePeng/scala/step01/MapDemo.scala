package com.JadePeng.scala.step01

import scala.collection.mutable

/**
  * Map映射
  * 无序, key不可重复
  *
  * 在scala中映射分为两类:
  * 可变长映射
  * 不可变长映射  长度和值不可再次改变
  *
  * @author Peng
  */
object MapDemo {
  def main(args: Array[String]): Unit = {
    /**
      * 1. 定义了一个不可边长的映射, 长度和值一旦初始化不可能再次被改变
      */
    //通过对偶元祖的方式创建
    val map0 = Map(("a", "A"), ("b", "B")) //Map(a -> A, b -> B)
    println(map0)
    //通过箭头的方式创建
    val map1 = Map("a" -> "A", "b" -> "B") //Map(a -> A, b -> B)
    println(map1)
    //通过混合方式创建映射
    val map2 = Map(("a", "A"), "b" -> "B")


    /**
      * 2. 可变长map映射，mutable包
      */
    val map4: mutable.Map[String, String] = mutable.Map(("a", "A"))
    //将value变成 AA
    map4("a") = "AA"
    //添加一对key,value
    map4 += ("b" -> "B")
    map4.put("c", "CC")
    map4 ++= mutable.Map(("d", "AC"))
    println("未删除前 \t" + map4)

    //删除元素
    map4 -= "a"
    map4.-=("a")
    map4.remove("a")
    println("删除后 \t" + map4)

    /**
      * 3. 映射常用的操作方法
      */
    //判断key是否存在, 返回true or false
    println(map4.contains("a"))

    //map4取值的时候，返回的数据类型是Option，里面有两个取值范围，一个是Some("Value")， 一个是None，some表示有值，none表示没有值
    //返回value Some(AC)
    println(map4.get("d"))
    //不存在返回None
    println(map4.get("e"))
    println(map4.getOrElse("x", ""))

    //遍历map当中的元素
    for (x <- map4) {
      println(x)
    }
    println("***************遍历map当中的元素***************")
    for ((k, v) <- map4) {
      println(k, v)
    }
    println("***************遍历map当中的key***************")
    for (y <- map4.keys) {
      println(y)
    }
    println("***************遍历map当中的values***************")
    for (y <- map4.values) {
      println(y)
    }

    //将对偶元祖的数组转换成map
    val arr = Array(("name", "zhangsan"), ("age", "23"))
    println(arr.toMap)

    //获取头部数据
    map4.head
    //获取尾部数据
    map4.last
    //获取最大值
    map4.max
    //获取最小值
    map4.min
    //获取长度
    map4.size

    val keys: Iterable[String] = map4.keys
    val key_iter = keys.iterator
    while (key_iter.hasNext) {
      val key = key_iter.next()
      println("key迭代器循环 \t" + key)
    }

    val filterKey = map4.keys.filter(x => x.equals("b"))
    println("key进行过滤  \t" + filterKey)

    val filterKey2 = map4.filterKeys(x => x.equals("c"))
    println("key进行过滤2  \t" + filterKey2)

    //查找
    val find = map4.find(x => {
      x._1.equals("b")
    })
    println("find方法查找key \t" + find)

    map4.foreach(x => println(x))

  }
}
