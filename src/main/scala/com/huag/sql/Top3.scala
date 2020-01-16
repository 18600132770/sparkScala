package com.huag.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author huag
  * @date 2019/12/12 9:57
  */
object Top3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Top3")

    val ssc = new StreamingContext(conf, Seconds(1))

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits

    val clickLogDstream = ssc.socketTextStream("spark1.cnki", 9999)

    val categoryProductPairsDStream = clickLogDstream.map{clickLog => (clickLog.split(" ")(2) + "_" + clickLog.split(" ")(1), 1)}

    val categoryProductCountsDStream = categoryProductPairsDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )

    categoryProductCountsDStream.foreachRDD(categoryProductCountsRDD => {
      val categoryProductCountRowRDD = categoryProductCountsRDD.map(tuple =>{
        val categroy = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(categroy, product, count)
      })

      val structType = StructType(Array(
        StructField("categroy", StringType, true),
        StructField("product", StringType, true),
        StructField("click_count", IntegerType, true)
      ))

      val categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDD, structType)

      categoryProductCountDF.createOrReplaceTempView("product_click_log")

      val top3ProductDF = spark.sql("" +
        "select category, product, click_count from (" +
          "select categroy, product, click_count, " +
          "row_number() over (partition by categroy order by click_count desc) rank " +
          "from product_click_log" +
          ") tmp " +
        "where rank <= 3")

      top3ProductDF.show()
    })

    ssc.start()

    ssc.awaitTermination()




  }

}
