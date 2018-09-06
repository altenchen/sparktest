package enn.enndigit.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.structuredstreaming
  */
object IoTUDAFWithTypedData {

  Logger.getLogger("org").setLevel(Level.ERROR)

//  case class DeviceData(device: String, deviceType: String, signal: Long, time: String)
  case class DeviceData(device: String, signal: Long)
  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[DeviceData, Average, Double] {

    def zero: Average = Average(0L, 0L)

    def reduce(buffer: Average, deviceData: DeviceData): Average = {
      buffer.sum += deviceData.signal
      buffer.count += 1
      buffer
    }

    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    def finish(reduction: Average) : Double = reduction.sum.toDouble / reduction.count

    def bufferEncoder: Encoder[Average] = Encoders.product

    def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  }

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .appName("UDAF Example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    import ss.implicits._
    val df: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/devicesimpledata.txt")
    val ds: Dataset[DeviceData] = df.as[DeviceData]
    ds.createOrReplaceTempView("devicetable")

    val averageSignal: TypedColumn[DeviceData, Double] = MyAverage.toColumn.name("average_signal")

    ss.sql("select * from devicetable").show()

    ds.select(averageSignal).show()

//    ss.sql("select averageSignal from devicetable").show()

  }
}
