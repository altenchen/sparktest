package enn.enndigit.toplink.sparkSQLAndDFAndDS

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/9
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.sparkSQLAndDFAndDS
  */
object SparkSQLExample {
  val ss = SparkSession
    .builder()
    .appName("sparkSQL basic example demo")
//    .config("spark.some.config.option", "some-value")
    .master("local[2]")
    .getOrCreate()
  import ss.implicits._

//  val df: DataFrame = ss.read.json("/Users/chenchen/Desktop/testdata/testjson.json")
////  df.show()
////
////  df.printSchema()
////
////  df.select("name").show()
////
////  df.select($"name", $"age" + 1).show()
////
////  df.filter($"age">21).show()
////
////  df.groupBy("age").count().show()
//
//  df.createOrReplaceTempView("People")
//
////  ss.sql("select name, age as aging from People").show()
//
  case class Person(name: String, age: Int)
//
//  val caseclassDS: Dataset[Person] = Seq(Person("kobe", 24)).toDS()
//
//  caseclassDS.show()
//
//  val number: Dataset[Int] = Seq(1,2,3).toDS()
//  number.map(_ + 1).show()

//  val path = "/Users/chenchen/Desktop/testdata/testjson.json"
//
//  val peopleDS: Dataset[Person] = ss.read.json(path).as[Person]
//  peopleDS.show()

//  val sc: SparkContext = ss.sparkContext
//  sc.setLogLevel("WARN")
//  val sourceData: RDD[Array[String]] = sc.textFile("/Users/chenchen/Desktop/testdata/sparksqldata/people.txt").map(_.split(","))
//  val peopleDF: DataFrame = sourceData.map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()
//  peopleDF.createOrReplaceTempView("People")
//  val teenagerDF: DataFrame = ss.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19;")
//  teenagerDF.map(teenager=> "Name:" + teenager.getAs[String]("name")).show()

  def runInferSchemaExample(ss: SparkSession) = {
    val peopleDF: DataFrame = ss.sparkContext
      .textFile("/Users/chenchen/Desktop/testdata/sparksqldata/people.txt")
      .map(_.split(","))
      .map(attribute => Person(attribute(0), attribute(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagerDF: DataFrame = ss.sql("select name, age from people where age between 10 and 20")

    //  teenagerDF.map(teenager=> "Name:" + teenager(0)).show()
    teenagerDF.map(teenager => "Name:" + teenager.getAs[String]("name")).show()

    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val pairValue: Array[Map[String, Any]] = teenagerDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()

    pairValue.foreach(println)
  }

  def runProgrammaticSchemaExample(ss: SparkSession) = {
    val peopleRDD: RDD[String] = ss.sparkContext.textFile("/Users/chenchen/Desktop/testdata/sparksqldata/people.txt")
    val schemaString = "name age"
    val fields: Array[StructField] = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attribute => Row(attribute(0), attribute(1).trim))

    val peopleDF: DataFrame = ss.createDataFrame(rowRDD, schema)

    peopleDF.createOrReplaceTempView("people")

    val results: DataFrame = ss.sql("select name,age from people")

    results.map(attributes => "Name:" + attributes.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val pairValue: Array[Map[String, Any]] = results.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()

    pairValue.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    runProgrammaticSchemaExample(ss)
  }


}
