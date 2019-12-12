package com.huag.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @author huag
  * @date 2019/12/11 18:38
  */
class StringCountUDAF extends UserDefinedAggregateFunction{

  /**
    * 输入数据的类型
    * @return
    */
  override def inputSchema: StructType = {
    StructType(Array(StructField("str", StringType, true)))
  }

  /**
    * 中间进行聚合时，处理的数据类型
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))
  }

  /**
    * 函数返回值的类型
    * @return
    */
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  /**
    * 为每个分组的数据执行初始化操作
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  /**
    * 每个分组对新进来的值进行分组对应的聚合值的计算
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  /**
    * 由于spark是分布式运行的，每一组的数据在不同的task运行，并进行局部聚合
    * 但是，最后最后一定要进行合并，也就是merge
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  /**
    * 每个分组的聚合值，通过中间的缓存聚合，最后返回一个最终的聚合值
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
