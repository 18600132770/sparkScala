package com.huag.sql

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
/**
  * 日志统计
  *
  * @author huag
  * @date 2019/12/11 10:32
  */
object DailySale {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DailySale")
      .getOrCreate()

    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")

    val sc = spark.sparkContext

    //使用sparkSession -> spark的内置函数
    import spark.implicits

    val rdd = sc.parallelize(userSaleLog, 16)

    val userSaleLogRowRDD = rdd.filter(_.split(",").length==3)
      .map(log => Row(log.split(",")(0), log.split(",")(1).toDouble))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)
    ))

    val userSaleLogDF = spark.createDataFrame(userSaleLogRowRDD, structType)

    userSaleLogDF.groupBy("date")
      .agg(max("date"), sum("sale_amount"))
//      .map(row => Row(row(1), row(2)))
      .collect()
      .foreach(println)


  }


}
