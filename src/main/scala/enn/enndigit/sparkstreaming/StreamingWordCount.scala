package enn.enndigit.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/7
  * @Project:sparktest
  * @Package:enn.enndigit.sparkstreaming
  */
object StreamingWordCount{
  def main(args: Array[String]) = {
//    if (args.length < 1) {
//      System.err.println("Usage: StreamingWordCount <directory>")
//      System.exit(1)
//    }

    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    //    val ssc = new StreamingContext(conf, Seconds(20))
    val sc: SparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
//    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
//    val sourceData: DStream[String] = ssc.textFileStream("/Users/chenchen/Desktop/testdata/")
    val sourceData: DStream[String] = ssc.textFileStream("/Users/chenchen/Desktop/testdata/")
//    val sourceData: RDD[String] = sc.textFile("/Users/chenchen/Desktop/testdata/word.txt")
    val lines: DStream[String] = sourceData.flatMap(_.split(" "))
//    val lines: RDD[String] = sourceData.flatMap(_.split(" "))
    val line: DStream[(String, Int)] = lines.map((_,1))
//    val line: RDD[(String, Int)] = lines.map((_,1))
    val result: DStream[(String, Int)] = line.reduceByKey(_+_)
//    val result: RDD[(String, Int)] = line.reduceByKey(_+_)

//    val tuples: Array[(String, Int)] = result.collect()
    result.print()

//    tuples.foreach(println)
    ssc.start()
    ssc.awaitTermination()
  }
}



