package com.huag.flink.streaming.customSourceTest

import com.huag.flink.streaming.customSource.MyParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huag
  * @date 2020/1/17 14:48
  */
object StreamingWithParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceStream = env.addSource(new MyParallelSourceScala).setParallelism(2)

    val map = sourceStream.map(line => {
      println("输出的数据：" + line)
      line
    })

    val sum = map.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute()

  }

}
