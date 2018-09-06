//package enn.enndigit.toplink.KafkaWordcount
//
//import enn.enndigit.toplink.StreamingExample
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/10
//  * @Project:sparktest
//  * @Package:enn.enndigit.toplink.KafkaWordcount
//  */
//object KafkaWordcount {
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 4) {
//      System.err.println("Usage: KafkaWordcount <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
//
//    StreamingExample.setStreamingLogLevels()
//
//    val Array(zkQuorum, group, topics, numThreads) = args
//    val conf: SparkConf = new SparkConf().setAppName("KafkaWordcount")
//    val interval: Int = 5
//    val ssc = new StreamingContext(conf, Seconds(interval))
//    ssc.checkpoint("/Users/chenchen/Desktop/tesetdata/kafkadata")
//
//    val topicMap = topics.spilt(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createDirectStream(ssc, zkQuorum, group, topicMap).map(_._2)
//
//  }
//}
