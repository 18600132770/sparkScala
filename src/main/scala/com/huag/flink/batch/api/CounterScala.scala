package com.huag.flink.batch.api

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author huag
  * @date 2020/1/16 16:44
  */
object CounterScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataset: DataSet[String] = env.fromElements("a", "b", "c", "d", "e")

    val resultDataset: DataSet[String] = dataset.map(new RichMapFunction[String, String] {

      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      override def map(value: String): String = {
        this.numLines.add(1)
        value
      }
    }).setParallelism(4)


    val filePath = "f://result.txt"
    resultDataset.writeAsText(filePath,  WriteMode.OVERWRITE)

    val jobResult: JobExecutionResult = env.execute(CounterScala.getClass.getName)
    val result: Int = jobResult.getAccumulatorResult[Int]("num-lines")

    print(result)



  }

}
