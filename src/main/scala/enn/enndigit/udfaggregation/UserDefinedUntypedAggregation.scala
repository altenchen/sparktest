package enn.enndigit.udfaggregation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/3
  * @Project:sparktest
  * @Package:enn.enndigit.udfaggregation
  */
object UserDefinedUntypedAggregation {

  object MyAverage extends UserDefinedAggregateFunction {
    //初始化inputSchema
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    //初始化中间结果
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    //返回值类型
    def dataType: DataType = DoubleType

    //Whether this function always returns the same output on the identical input（幂等性）
    def deterministic: Boolean = true
    //初始化给定的中间变量 buffer，初始化为零值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }
    //更新单个 buffer
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }
    //合并两个 buffer，buffer1+buffer2
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    //基于聚合 buffer，计算出最终结果
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)

  }

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      val ss: SparkSession = SparkSession
        .builder()
        .appName("UserDefinedUntypedAggregation Example")
        .master("local[2]")
        .enableHiveSupport()
        .getOrCreate()

      //注册 UDAF 函数
      ss.udf.register("myAverage", MyAverage)

      val dataFrame: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/employees.json")

      dataFrame.createOrReplaceTempView("employee")

      ss.sql("select * from employee").show()

      ss.sql("select myAverage(salary) as employee_salary from employee").show()
    }
}
