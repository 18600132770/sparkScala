package com.huag.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author huag
  * @date 2019/12/11 14:49
  */
object DatasetOperation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DatasetOperation")
      .getOrCreate()

    import spark.implicits

    val dataset = spark.read.option("header", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")csv("hdfs://spark1.cnki:8020//DataAnalyse/分类/input/预测鲍鱼年龄.csv")

    dataset.show()

    dataset.printSchema()

    dataset.select("Sex").show()

    dataset.select(dataset("Sex"), dataset("Sex")+1).show()

    dataset.select(col("Sex"), col("Sex")+1).show()

    dataset.select(col("Sex").gt(0)).show()

    dataset.filter(col("Sex") > 0).show()

    dataset.select("Sex").filter(col("Sex") > 0).write.save("hdfs://spark1.cnki:8020/DataAnalyse/测试数据/1")

    spark.close()

  }

}
