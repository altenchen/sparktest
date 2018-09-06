package enn.enndigit.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.structuredstreaming
  */
object IoTUDAFWithUntypedData {


  object ComputeAverage extends UserDefinedAggregateFunction {

    //需要重写inputSchema, bufferSchema, dataType, deterministic, initialize, update, merge, evaluate
    //inputSchema
    override def inputSchema: StructType = StructType(StructField("inputSchema", LongType) :: Nil)

    //bufferSchema
    override def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) ::Nil)
    }

    //dataType
    override def dataType: DoubleType = DoubleType

    //deterministic
    override def deterministic: Boolean = true

    //initialize
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    //update
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    //merge
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //evaluate
    override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)

  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession
      .builder()
      .appName("IoTUDAFWithUntypedData Example")
      .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()

    import ss.implicits._
    val df: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/devicedata.txt")

    ss.udf.register("computeAverage", ComputeAverage)

    df.createOrReplaceTempView("IotTable")

    ss.sql("select * from IotTable").show()

    ss.sql("select computeAverage(signal) as averagesignal from IotTable").show()

    ss.stop()

  }
}
