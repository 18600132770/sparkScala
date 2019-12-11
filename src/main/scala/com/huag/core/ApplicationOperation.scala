package com.huag.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author huag
  * @date 2019/12/10 21:04
  */
object ApplicationOperation {

  def main(args: Array[String]): Unit = {

//    reduce()
//    collect()
      countByKey()
  }

  def reduce(): Unit ={
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("reduce")

    val sparkContext = new SparkContext(conf)

    val array = Array(1, 2, 3, 4, 5)
    val rdd = sparkContext.parallelize(array, 6)

    val count = rdd.reduce(_+_)
    println(count)
  }

  def collect(): Unit ={
    val conf = new SparkConf()
      .setAppName("collect")
      .setMaster("local")

    val sparkContext = new SparkContext(conf)
    val array = Array(1, 2, 3, 4, 5)

    val rdd = sparkContext.parallelize(array, 10)
    val mapRdd = rdd.map(_*2)
    val collect = mapRdd.collect()
    collect.foreach(println(_))

    for(co <- collect){
      println(co)
    }

    println(rdd.count())

    val take = rdd.take(3)
    for(t <- take){
      println(t)
    }

  }


  def countByKey(): Unit ={
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("countByKey")

    val sc = new SparkContext(conf)

    val arrray = Array(("class1", 2), ("class2", 1), ("class1", 3))

    val rdd = sc.parallelize(arrray, 6)

    val countByKey = rdd.countByKey()

    println(countByKey)

  }

}
