package com.huag.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * @author huag
  * @date 2019/12/11 9:47
  */
object SecondSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SecondSort")

    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile("hdfs://spark1.cnki:8020/DataAnalyse/近义词/db/1.7万条近义词替换.csv", 16)

    fileRDD.flatMap(_.split(","))
      .map(word => (new SecondSortKey(word.hashCode,Random.nextInt(100)), word))
      .sortByKey(false)
      .map(tuple => (tuple._2, tuple._1))
      .foreach(tuple => println(tuple._1 + ": " +  tuple._2.first + "\t" + tuple._2.second))

  }

}
