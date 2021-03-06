package com.huag.sql

import org.apache.spark.sql.SparkSession

/**
  * 如何想在scala开发的spark中实现基于反射的RDD到DataFrame的转换，就必须用object extends App的方式
  * 不能用def main()的方式，否则会报no typetag for ... class
  * @author huag
  * @date 2019/12/11 17:16
  */
object SparkRunReflection extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("SparkRunReflection")
    .getOrCreate()

  val studentDataset = spark.read.json("/DataAnalyse/测试数据/students.json")

  import spark.implicits

  case class Student(id: Long, name: String, age: Long)

  val studentRDD = studentDataset.rdd

  studentRDD.map(row => Student(row.getAs[Long]("id"), row.getAs[String]("name"), row.getAs[Long]("age")))
    .collect()
    .foreach{
      stu => println(stu.id + ": " + stu.name + ": " +stu.age)
    }

  studentRDD.map(row =>{
    val map = row.getValuesMap[Any](Array("id", "name", "age"))
    Student(map("id").toString().toLong, map("name").toString, map("age").toString().toLong)
  }).collect().foreach(println)




}
