//package com.huag.streaming
//
//import java.io.{BufferedReader, InputStreamReader}
//
//import org.apache.spark.SparkConf
//import org.apache.spark.internal.Logging
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.receiver.Receiver
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import java.net.Socket
//import java.nio.charset.StandardCharsets
//
///**
//  * @author huag
//  * @date 2019/12/12 10:50
//  */
//class CustomReceiver {
//
//  def main(args: Array[String]): Unit = {
//
//    StreamingExamples.setStreamingLogLevels()
//
//    val conf = new SparkConf()
//      .setAppName("CustomReceiver")
//      .setMaster("local")
//
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//
////    val lines = ssc.receiverStream(new CustomReceiver("spark1.cnki", 9999))
////    new CustomReceiver("1", 2)
//
//  }
//
//}
//
//class CustomReceiver(host: String, port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{
//
//  /**
//    * 启动通过连接获取数据的线程
//    */
//  def onStart(): Unit = {
//    new Thread("Socket Receiver"){
//      override def run(): Unit = {
//        receive()
//      }
//    }.start()
//  }
//
//  /**
//    * 这与调用receive（）的线程没什么关系，旨在自行停止isStopped（）返回false
//    */
//  def onStop(): Unit = {
//
//  }
//
//  /**
//    * 创建一个连接，接受数据，直到连接中断为止
//    */
//  private def receive(): Unit ={
//    var socket: Socket = null
//    var userInput: String = null
//    try{
//      logInfo("Connecting to " + host + ": " + port)
//      socket = new Socket(host, port)
//      logInfo("Connected to " + host + ":" + port)
//      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
//      userInput = reader.readLine()
//      while (!isStopped() && userInput != null){
//        store(userInput)
//        userInput = reader.readLine()
//      }
//      reader.close()
//      socket.close()
//      logInfo("Stopped receiving")
//      restart("Trying to connect again")
//    }catch {
//      case e: java.net.ConnectException =>
//        restart("Error connecting to " + host + ":" + port, e)
//      case t: Throwable =>
//        restart("Error receiving data", t)
//    }
//
//  }
//
//
//}
