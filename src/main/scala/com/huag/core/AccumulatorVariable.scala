package com.huag.core

import org.apache.spark.sql.SparkSession

/**
  * @author huag
  * @date 2019/12/10 20:06
  */
object AccumulatorVariable {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession
      .builder()
      .master("local")
      .appName("AccumulatorVariable")
      .getOrCreate()

    var sparkContext = spark.sparkContext

    var array = Array(1, 2, 3, 4, 5)

    var dataset = sparkContext.parallelize(array, 10)

    var ac = sparkContext.accumulator(0)
    dataset.foreach(num => ac.add(num))
    dataset.foreach(ac+=_)

    val ac1 = sparkContext.longAccumulator("longAccumulator")
    dataset.foreach(ac1.add(_))
    println(ac1.name + ": " + ac1.value)

    val doubleAccumulator = sparkContext.doubleAccumulator("doubleAccumulator")
    dataset.foreach(doubleAccumulator.add(_))
    println(doubleAccumulator.name + ": " + doubleAccumulator.value)


  }

}
