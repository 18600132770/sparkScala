package com.huag.flink.streaming.api

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huag
  * @date 2020/1/17 14:28
  */
object UnionScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val streamSource1: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)
    val streamSource2: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)

    val unionStream: DataStream[Long] = streamSource1.union(streamSource2)

    unionStream.timeWindowAll(Time.seconds(2), Time.seconds(1)).sum(0).print().setParallelism(1)

    env.execute()
    



  }

}
