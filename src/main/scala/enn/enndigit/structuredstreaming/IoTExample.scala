package enn.enndigit.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.joda.time.DateTime

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/4
  * @Project:sparktest
  * @Package:enn.enndigit.structuredstreaming
  */
object IoTExample {

//  case class DeviceData(device: String, deviceType: String, signal: Option[Double], time: String)
  case class DeviceData(device: String, deviceType: String, signal: Double, time: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession
      .builder()
      .appName("IoTExample")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val initDF: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/devicedata.txt")
    println("initDF schema")
    initDF.printSchema()

    import ss.implicits._
    val initDS: Dataset[DeviceData] = initDF.as[DeviceData]
    println("initDS schema")
    initDS.printSchema()

    initDF.createOrReplaceTempView("update")
    ss.sql("select * from update").show()
    ss.sql("select * from update where signal > 10").show()
    ss.sql("select device,avg(signal) from update").show()
//    ss.sql("select avg(signal) from update")

    //求 signal 大于10的数据
    initDF.select("device").where("signal > 10").show()    //untyped API
    initDS.filter(_.signal > 10).map(_.device)  //typed API
//    initDS.filter(_.signal.getOrElse(false)>10).map(_.device)
//    initDS.filter(_.signal.map(_>10).getOrElse(false)).map(_.device).show()
//    initDS.filter{
//      case Some(signal) => signal > 10
//      case _ => false
//    }.map(_.device).show()

    initDF.groupBy("deviceType").count().show()

    import org.apache.spark.sql.expressions.scalalang.typed
    //using typed API 求 signal 的均值
//    initDS.groupByKey(_.deviceType).agg(typed.avg(_.signal.getOrElse(false))).show()

//    initDS.groupByKey(_.deviceType).agg(typed.avg(x=> {
//      if (x.signal == None)
//        false
//      else
//        x.signal.getOrElse(false)
//    })).show()
//
//    initDS.groupByKey(_.deviceType).agg(typed.avg(_.signal.map(x=> {
//      case Some(signal) => signal
//      case _ => false
//    }))).show()

    initDS.groupByKey(_.deviceType).agg(typed.avg(_.signal))


  }
}
