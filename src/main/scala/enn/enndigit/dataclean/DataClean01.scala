//package enn.enndigit.dataclean
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/3
//  * @Project:sparktest
//  * @Package:enn.enndigit.dataclean
//  */
//
//case class MatchData(id_1: Int, id_2: Int, rowscores: Array[Double], matched: Boolean)
//
//object DataClean01 {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("dataclean01").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val rowblocks: RDD[String] = sc.textFile("/Users/chenchen/Desktop/testdata/linkage")
//    val header: String = rowblocks.first
////    header.foreach(print)
//    val noheader: RDD[String] = rowblocks.filter(!_.contains("id_1"))
//
////    val noheader: RDD[String] = rowblocks.filter(!_.startsWith("id_1"))
//
//    def parse(line: String) = {
//      val pieces: Array[String] = line.split(",")
//      val id_1: Int = pieces(0).toInt
//      val id_2: Int = pieces(1).toInt
//
//      def toDouble(s: String) = {
//        if ("?".equals(s)) Double.NaN else s.toDouble
//      }
//
//      val matched: Boolean = pieces(11).toBoolean
//      val rowscores: Array[Double] = pieces.slice(2,11).map(toDouble)
//
//      MatchData(id_1, id_2, rowscores, matched)
//    }
//
//    //解析过得数据
//    val parsed: RDD[MatchData] = noheader.map(parse).unpersist(blocking = true)
////    parsed.foreach(print)
//
//    val rowdatas: RDD[Array[Double]] = parsed.map(_.rowscores)
//    val resultRowdatas: Array[Array[Double]] = rowdatas.collect
//    val data01: Array[Array[Double]] = rowdatas.take(100)
//    // 处理rowdata
//    var elemList: ArrayBuffer[(String, Double)] = new ArrayBuffer()
//
//    for (row <- data01) {
//      println()
//      for (elem <- row) {
//        elem match {
////          case 1.0 => elem = 100.0
////          case _ => elem = 0.0
//          case 1.0 => 100.0
//          case _ => 0.0
////          elemList += (elem)
//        }
//        print(elem)
//      }
//    }
//
//
//  }
//}
