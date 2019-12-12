package com.huag.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author huag
  * @date 2019/12/11 19:23
  */
object UDAFOperation extends App{

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("UDADOperation")
    .getOrCreate()

  val dataset = spark.read.option("header", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv("hdfs://spark1.cnki:8020/DataAnalyse/近义词/db/1.7万条近义词替换.csv")

  spark.udf.register("stringCountUDAF", new StringCountUDAF)

  dataset.createOrReplaceTempView("t1")

  spark.sql("select baidu, stringCountUDAF(baidu) as count from t1 group by baidu")
    .orderBy(desc("count"))
    .show()



}
