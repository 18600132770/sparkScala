package com.huag.flink.streaming.watermark

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * @author huag
  * @date 2020/1/17 15:21
  */
object StreamingWindowWatermarkScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketStream = env.socketTextStream("bigdata1.cnki", 9999, '\n')

    val inputMap = socketStream.map(line => {
      val array = line.split(",")
      (array(0), array(1).toLong)
    })

    val watermarkStream: DataStream[(String, Long)] = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {

      var currentMaxTimeStamp = 0L
      var maxOutOfOrderness = 10000L
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimeStamp - maxOutOfOrderness)

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
        val id = Thread.currentThread().getId
        println("currentThreadId: " + id + " ,key: " + element._1 + " ,elementTime: [" + element._2 + "|" + sdf.format(element._2) + "] ,currentMaxTimestamp:" +
          "" + currentMaxTimeStamp + "|" + sdf.format(currentMaxTimeStamp) + "], watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")

        timestamp
      }
    })

    val window = watermarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunction[Tuple2[String, Long], String, Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val keyStr = key.toString
          val arrayBuffer = ArrayBuffer[Long]()
          val iterator = input.iterator
          while (iterator.hasNext){
            val tuple2 = iterator.next()
            arrayBuffer.append(tuple2._2)
          }
          val array = arrayBuffer.toArray
          Sorting.quickSort(array)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          val result = keyStr + "," + array.length + "," + sdf.format(array.head) + "," + sdf.format(array.last)+ "," +
            sdf.format(window.getStart) + "," + sdf.format(window.getEnd)
          out.collect(result)

        }
      })

    window.print()

    env.execute()

  }



}
