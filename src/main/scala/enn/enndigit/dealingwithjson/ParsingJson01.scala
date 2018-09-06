//package enn.enndigit.dealingwithjson
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.ShortTypeHints
//import org.json4s.jackson.Serialization
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/6
//  * @Project:sparktest
//  * @Package:enn.enndigit.dealingwithjson
//  */
//object ParsingJson01 extends App {
//  case class NovelInfo(name:String, time:String, info:String, upmark:String, downmark:String)
//  def parser() = {
//
//    implicit val formats = Serialization.formats(ShortTypeHints(List()))
//    val conf: SparkConf = new SparkConf().setAppName("parsejson01").setMaster("local[2]")
//    val sc: SparkContext = new SparkContext(conf)
//    val input: RDD[String] = sc.textFile("/Users/chenchen/Desktop/testdata/testjson.json")
//    input.collect().foreach(x=> print(x + ","))
//    println(" ")
//    val first: String = input.take(1)(0)
//    println(first)
//    println(first.getClass)
//    val novel = parse(first).extract[NovelInfo]
//    println(novel.name)
//
//
//
//
//  }
//}
//
////{"user":".三爷.","time":"2016-05-29 18:09:56","body":"Dierji第二季呢？","comment_up":"0","comment_rep":"0"}
////object CC {
////
////  case class Person(name: String, age: Int)
////
////  def my() {
////    implicit val formats = Serialization.formats(ShortTypeHints(List()))
////    val input = sc.textFile("file:///home/user/sparktemp/testjson.json")
////    input.collect().foreach(x => print(x + ","))
////    println(" ")
////    val first = input.take(1)(0)
////    println(first)
////    println(first.getClass)
////    val p = parse(first).extract[Person]
////    println(p.name)
////    println("==========")
////    input.collect().foreach(x => {
////      var c = parse(x).extract[Person];
////      println(c.name + "," + c.age)
////    })
////  }
////}