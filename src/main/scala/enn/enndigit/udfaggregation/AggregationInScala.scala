package enn.enndigit.udfaggregation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.udfaggregation
  */
object AggregationInScala {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class DeviceData(device: String, deviceType: String, signal: Long)

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .appName("AggregationInScala Example")
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()
    import ss.implicits._
    val df: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/devicesimpledata.txt")

//    df.map(x=> DeviceData(x._1,x._2,x._3,x._4)).foreach(println)

//    val ds: Dataset[Row] = df.as[DeviceData].map(DeviceData=> Row(DeviceData(1), DeviceData(2), DeviceData(3).toLong, DeviceData(4)))
    val ds: Dataset[DeviceData] = df.map(x=> DeviceData(x(1).toString, x(2).toString, x(3).toString.toLong))

    ds.foreach(x=> {
//      x.device.foreach(println)
      x.device.foreach(println)
    })

//    df
//      .map(a => (a._1, (a._2, 1)))
//      .reduceByKey((a,b) => (a._1+b._1,a._2+b._2))
//      .map(t => (t._1,t._2._1/t._2._2))




  }
}

