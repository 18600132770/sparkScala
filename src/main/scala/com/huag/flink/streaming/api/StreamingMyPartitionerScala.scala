package com.huag.flink.streaming.api

import com.huag.flink.streaming.customSource.MyNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author huag
  * @date 2020/1/17 9:58
  */
object StreamingMyPartitionerScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    import org.apache.flink.api.scala._

    val dataSource: DataStream[Long] = env.addSource(new MyNoParallelSourceScala)

    val tupleData: DataStream[Tuple1[Long]] = dataSource.map(line => Tuple1(line))

    val partitionData: DataStream[Tuple1[Long]] = tupleData.partitionCustom(new MyPartitionerScala, 0)

    val result: DataStream[Long] = partitionData.map(line => {
      println("当前线程id：" + Thread.currentThread().getId + ", value = " + line)
      line._1
    })

    result.print().setParallelism(1)

    env.execute()




  }

}
