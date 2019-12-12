package com.huag.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author huag
  * @date 2019/12/11 20:38
  */
object HDFSFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("HDFSFile")

    val ssc = new StreamingContext(conf, Seconds(5))

    val line= ssc.textFileStream("hdfs://spark1.cnki:8020/DataAnalyse/1513/input/")

    val words = line.flatMap { _.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
