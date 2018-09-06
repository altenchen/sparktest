package enn.enndigit.udfaggregation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/3
  * @Project:sparktest
  * @Package:enn.enndigit.udfaggregation
  */
object UserDefinedTypedAggregation {

  case class Employee(name: String, salary: Long)
  case class Average(var sum: Long, var count: Long)

  object MyAverage extends Aggregator[Employee, Average, Double] {
    def zero: Average = Average(0L, 0L)

    def reduce(buffer: Average, employee: Employee) : Average = {
      buffer.sum += employee.salary
      buffer.count += 1
      buffer
    }

    def merge(b1: Average, b2: Average) : Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    def finish(reduction: Average) : Double = reduction.sum.toDouble / reduction.count

    def bufferEncoder: Encoder[Average] = Encoders.product

    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL user-defined Datasets aggregation example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    import ss.implicits._
    val initDS: Dataset[Employee] = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/employees.json").as[Employee]
    initDS.createOrReplaceTempView("employee")
    ss.sql("select * from employee").show()

    val averageSalary: TypedColumn[Employee, Double] = MyAverage.toColumn.name("average_salary")

    val result = initDS.select(averageSalary)
    result.show()
    ss.stop()
  }

}
