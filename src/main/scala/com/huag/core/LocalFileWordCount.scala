package com.huag.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author huag
  * @date 2019/12/11 9:20
  */
object LocalFileWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LocalFileWordCount")

    val sc = new SparkContext(conf)
    val fileRDD = sc.textFile("file:///D:\\mylog.log")

    val flatMap = fileRDD.flatMap(_.split(" "))

    val tupleRDD = flatMap.map((_,1))

    tupleRDD.reduceByKey(_+_)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(false)
      .map(tuple => (tuple._2, tuple._1))
      .foreach(println(_))




  }

}
