package com.huag.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author huag
  * @date 2019/12/11 8:56
  */
object HDFSFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("HDFSFile")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://spark1.cnki:8020/DataAnalyse/近义词/db/1.7万条近义词替换.csv")

    val flatRDD = rdd.flatMap(_.split(","))

    val tupleRDD = flatRDD.map((_,1))

    val reduceByKey = tupleRDD.reduceByKey(_+_)

//    reduceByKey.foreach(println(_))

    val reverseRDD = reduceByKey.map(tuple => (tuple._2, tuple._1))

    val sortRDD = reverseRDD.sortByKey(false)

    sortRDD.map(tuple => (tuple._2, tuple._1)).foreach(println(_))


  }

}
