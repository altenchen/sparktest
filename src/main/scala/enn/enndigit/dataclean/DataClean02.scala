package enn.enndigit.dataclean

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/6
  * @Project:sparktest
  * @Package:enn.enndigit.dataclean
  */

case class MatchData(id_1: Int, id_2: Int, innerdata: Array[Double], matched: Boolean)

object DataClean02 extends App{
  val conf: SparkConf = new SparkConf().setAppName("dataclean02").setMaster("local[2]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val rowsblock: RDD[String] = sc.textFile("/Users/chenchen/Desktop/testdata/linkage/")
  val header: String = rowsblock.first
//  print(header)
  val noheader: RDD[String] = rowsblock.filter(!_.contains(header))
  val top10Header: Array[String] = noheader.take(10)
//  top10Header.foreach(println)
  def parser(line: String) = {
    val pieces: Array[String] = line.split(",")
    val id_1: Int = pieces(0).toInt
    val id_2: Int = pieces(1).toInt
    val matched: Boolean = pieces(11).toBoolean
    def toDouble(num: String) = {
      if (num.equals("?")) Double.NaN else num.toDouble
    }
    val innerdata: Array[Double] = pieces.slice(2,11).map(toDouble)
    MatchData(id_1, id_2, innerdata, matched)
  }
  val parsed: RDD[MatchData] = noheader.map(parser)
  val broadcast: Broadcast[Array[MatchData]] = sc.broadcast(parsed.collect())
  print(broadcast.value)


  val collect: Array[MatchData] = parsed.collect
  val cass: Broadcast[Array[MatchData]] = sc.broadcast(collect)
  println(cass)

  val parsedTop10: Array[MatchData] = parsed.take(5)
//  parsedTop10.foreach(println)
  val id_1: RDD[Int] = parsed.map(_.id_1)

  val id_11: Array[Int] = parsed.map(_.id_1).collect()


  val id_2: RDD[Int] = parsed.map(_.id_2)
  val rows: RDD[Array[Double]] = parsed.map(_.innerdata)
  val rows_11: Array[Array[Double]] = rows.collect
  val rows_1: RDD[Double] = rows.map(x=>x(1))









  
  
}
