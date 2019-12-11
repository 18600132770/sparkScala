package com.huag.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author huag
  * @date 2019/12/11 8:49
  */
object BroadcastVariable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("BroadcastVariable")

    val sc = new SparkContext(conf)

    val array = Array(1, 2, 3, 4, 5)

    val rdd = sc.parallelize(array, 1)

    val bc = sc.broadcast(2)

    rdd.map(_*bc.value).foreach(println(_))



  }

}
