package com.huag.flink.streaming.socket

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author huag
  * @date 2020/1/17 15:00
  */
object SocketSourceWindowWordcount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val hostname = "bigdata1.cnki"
    val port = 9999
    val delimiter = '\n'

    val socketStream: DataStream[String] = env.socketTextStream(hostname, port, delimiter)

    val windowWordcount = socketStream.flatMap(line => line.split("\n"))
      .map(word => WordWithCount(word, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1)

    windowWordcount.print().setParallelism(1)

    env.execute()


  }

  case class WordWithCount(word: String, num: Integer)

}
