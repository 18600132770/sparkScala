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


    spark.sql("select * from kg.nlp_doc").show()

  }

}
