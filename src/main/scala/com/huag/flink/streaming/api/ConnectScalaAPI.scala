package com.huag.flink.streaming.api

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * @author huag
  * @date 2020/1/17 9:17
  */
object ConnectScalaAPI {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val text1 = env.addSource(new MyNoParallelSourceScala)
    val text2 = env.addSource(new MyNoParallelSourceScala)

    val steram1: DataStream[String] = text1.map(line => "str_" + line )

    val connectStream: ConnectedStreams[String, Long] = steram1.connect(text2)

    val mapStream: DataStream[Any] = connectStream.map(f0 => f0, f1 => f1)

//    mapStream.print()

    env.execute()



  }

}
