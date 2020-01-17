package com.huag.flink.streaming.customSourceTest

import com.huag.flink.streaming.customSource.MyRichParallelSourceScala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huag
  * @date 2020/1/17 14:51
  */
object StreamingWIthRichParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceStream = env.addSource(new MyRichParallelSourceScala).setParallelism(4)

    val map = sourceStream.map(line => {
      println("接收到的数据：" + line)
      line
    })

    val sum = map.timeWindowAll(Time.seconds(2)).sum(0)

    sum.print().setParallelism(1)

    env.execute()

  }

}
