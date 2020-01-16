package com.huag.flink.batch.api

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * @author huag
  * @date 2020/1/16 17:06
  */
object BroadcastScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val broadData =ListBuffer[Tuple2[String, Int]]()
    broadData.append(("Tom", 1))
    broadData.append(("Jerry", 2))
    broadData.append(("Lucy", 3))

    val tupleDataset: DataSet[(String, Int)] = env.fromCollection(broadData)
//    val toBroadcastDataset: DataSet[(String, Int)] = tupleDataset.map(tuple => (tuple._1, tuple._2))
    val toBroadcastDataset = tupleDataset.map(tuple =>{
      Map(tuple._1 -> tuple._2)
    })

    val dataset: DataSet[String] = env.fromElements("Tom", "Jerry", "Lucy")

    val result: DataSet[String] = dataset.map(new RichMapFunction[String, String] {

      var listData: java.util.List[Map[String, Int]] = null
      var allMap = Map[String, Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String, Int]]("broadcastMapName")
        val iterator = listData.iterator()
        while (iterator.hasNext) {
          val next = iterator.next()
          allMap = allMap.++(next)
        }
      }

      override def map(value: String): String = {
        val age = allMap.get(value).get
        value + "," + age
      }
    }).withBroadcastSet(toBroadcastDataset, "broadcastMapName")

    result.print()

//    env.execute(BroadcastScala.getClass.getName)

  }

}
