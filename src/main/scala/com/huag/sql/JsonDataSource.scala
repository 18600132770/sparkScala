package com.huag.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author huag
  * @date 2019/12/11 16:15
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("JsonDataSource")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

//    spark.read.json("hdfs://spark1.cnki:8082//DataAnalyse/测试数据/students.json")
//    spark.read.json("file:///d:\\hadoop.json")
    val dataset = spark.read.json("/DataAnalyse/测试数据/students.json")

    dataset.createOrReplaceGlobalTempView("students")

    val excellentStudentsDataset = spark.sql("select name, score from global_temp.students where score >= 80")

    excellentStudentsDataset.show()

    val excellentStudentNames = excellentStudentsDataset.select("name").collect()

    excellentStudentNames.foreach(println(_))

    val studentInfoJSON = Array(
      "{\"name\":\"leo\",\"age\":18}",
      "{\"name\":\"marry\",\"age\":22}",
      "{\"name\":\"tom\",\"age\":15}"
    )

    val sc = spark.sparkContext
    import spark.implicits

    val studentInfoRDD = sc.parallelize(studentInfoJSON, 6)
    val studentInfoDataset = spark.read.json(studentInfoRDD)

    studentInfoDataset.createOrReplaceGlobalTempView("studentInfo")

    var sql = "select name, age from global_temp.studentInfo where name in ("

    for(i <- 0 until excellentStudentNames.length){
      sql += "'" + excellentStudentNames(i).get(0) + "'"
      if(i < excellentStudentNames.length - 1){
        sql += ","
      }
    }
    sql += ")"

    spark.sql(sql).show()

    studentInfoDataset.join(excellentStudentsDataset,
      studentInfoDataset.col("name").equalTo(excellentStudentsDataset.col("name")))
      .show()

    studentInfoDataset.joinWith(excellentStudentsDataset,
      studentInfoDataset.col("name").equalTo(excellentStudentsDataset.col("name")),
      "left").show()

    studentInfoDataset.write.format("json").save("/DataAnalyse/测试数据/studentInfo")


  }

}
