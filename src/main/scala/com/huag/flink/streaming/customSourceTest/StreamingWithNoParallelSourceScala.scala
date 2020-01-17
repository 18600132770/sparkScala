package com.huag.flink.streaming.customSourceTest

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huag
  * @date 2020/1/17 14:44
  */
object StreamingWithNoParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceStream: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)

    val map = sourceStream.map(line => {
      println("接收到的数据： " + line)
      line
    })

    val sum = map.timeWindowAll(Time.seconds(2), Time.seconds(1)).sum(0)

    sum.print().setParallelism(1)

    env.execute()

  }

}
