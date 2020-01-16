package com.huag.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author huag
  * @date 2020/1/16 14:46
  */
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.readTextFile("f:\\Hadoop学习笔记.txt")

    text.print()

    import org.apache.flink.api.scala._

    val resultDataset: AggregateDataSet[(String, Int)] = text.flatMap(_.toLowerCase().split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    resultDataset.writeAsCsv("f:\\result.txt", "\n", " ", WriteMode.OVERWRITE).setParallelism(1)

    env.execute()

  }

}
