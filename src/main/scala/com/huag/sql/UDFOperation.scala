package com.huag.sql

import org.apache.spark.sql.SparkSession

/**
  * @author huag
  * @date 2019/12/11 18:11
  */
object UDFOperation extends App{

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("UDFOperation")
    .getOrCreate()

  //自定义函数，可以在spark sql里直接用
  def lengthLongerThan(str: String, length: Int): Boolean = str.length > length

  spark.udf.register("lengthLongerThan", lengthLongerThan _)

  val dataset = spark.read.json("/DataAnalyse/测试数据/students.json")

  dataset.createOrReplaceTempView("student")

  spark.sql("select name, lengthLongerThan(name, 3) from student").show()

  spark.sql("select name from student where lengthLongerThan(name, 3)").show()

}
