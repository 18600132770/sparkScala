package com.huag.flink.streaming.api

import java.{lang, util}

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
  * @author huag
  * @date 2020/1/17 14:21
  */
object StreamingSplitScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceStream: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)

    val splitStream: SplitStream[Long] = sourceStream.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })


    splitStream.print()

//    splitStream.select("odd").print().setParallelism(1)

    env.execute()

  }

}
