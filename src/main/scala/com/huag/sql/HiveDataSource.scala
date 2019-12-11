package com.huag.sql

import org.apache.spark.sql.SparkSession

/**
  * @author huag
  * @date 2019/12/11 15:06
  */
object HiveDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveDataSource")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

//    spark.sql("select * from kg.nlp_doc").show()

    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    spark.sql("USE test")

//    spark.sql("DROP TABLE IF EXISTS userLowCarbon")
    spark.sql("CREATE TABLE IF NOT EXISTS userLowCarbon (" +
      "user_id string," +
      "data_dt string," +
      "low_carbon int" +
      ")" +
      "row format delimited fields terminated by '\\t'")

//    spark.sql("LOAD DATA LOCAL INPATH  '/home/spark-study/hive/user_low_carbon.txt' INTO TABLE userLowCarbon")

    val dataset = spark.sql("SELECT * from userLowCarbon")

    dataset.createOrReplaceGlobalTempView("userLowCarbon")

    spark.sql("select * from global_temp.userLowCarbon").show()



  }

}
