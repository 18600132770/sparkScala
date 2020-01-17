package com.huag.flink.streaming.api

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author huag
  * @date 2020/1/17 9:46
  */
object FilterScalaAPI {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceStream: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)

    val filterStream: DataStream[Long] = sourceStream.filter(_%2==0)

    filterStream.print()

    env.execute()

  }

}
