//package enn.enndigit.singletoninscala
//
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/27
//  * @Project:sparktest
//  * @Package:enn.enndigit.singletoninscala
//  */
//object SparkContextSingleton {
//
//  @transient private var instance: SparkContext = _
//
//  def getInstance(sc: SparkConf) : SparkContext = {
//    if (instance == null) {
//      instance = SparkContext(sc)
//    }
//  }
//}
