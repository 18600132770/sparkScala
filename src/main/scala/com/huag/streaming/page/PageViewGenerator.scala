//package com.huag.streaming.page
//
//import java.util.Random
//
///**
//  * @author huag
//  * @date 2019/12/13 9:06
//  */
//
//class PageView(val url:String, val status: Int, val zipCode: Int, val userID: Int) extends Serializable{
//  override def toString: String = {
//    "%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID)
//  }
//}
//
//object PageView extends Serializable{
//  def fromString(in: String): PageView = {
//    val parts = in.split("\t")
//    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
//  }
//}
//
//object PageViewGenerator {
//
//  val pages = Map("http://foo.com/" -> .7,
//  "http://foo.com/news" -> 0.2,
//    "http://foo.com/contact" -> .1
//  )
//
//  val httpStatus = Map(200 -> .95,
//    404 -> .05)
//
//  val userID = Map((1 to 100).map(_ -> .01): _*)
//
//  def pickFromDistribution[T](inputMap: Map[T, Double]): T = {
//    val rand = new Random().nextDouble()
//    var total = 0.0
//    for((item, prob) <- inputMap){
//      total += prob
//      if(total > rand){
//        return item
//      }
//    }
//    inputMap.take(1).head._1
//  }
//
//  def getNextClickEvent(): String = {
//    val id = pickFromDistribution(userID)
//  }
//
//}
