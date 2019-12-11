package com.huag.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
/**
  * @author huag
  * @date 2019/12/11 11:39
  */
object DailyUV {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DailyUV")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits

    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123"
    )

    val userAccessLogRDD = sc.parallelize(userAccessLog, 16)

    val mapRDD = userAccessLogRDD.map(log => (Row(log.split(",")(0), log.split(",")(1).toInt)))

    val structType =StructType(Array(
      StructField("date", StringType, true),
      StructField("userId", IntegerType, true)
    ))

    val df = spark.createDataFrame(mapRDD, structType)

//    df.groupBy("date")
//      .agg('date, countDistinct('userid))
//      .map { row => Row(row(1), row(2)) }
//      .collect()
//      .foreach(println)


  }

}
