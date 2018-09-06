package enn.enndigit.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{Metadata, StringType, StructField, StructType}
import org.apache.spark.sql._

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/3
  * @Project:sparktest
  * @Package:enn.enndigit.sparksql
  */

object SparkSqlExample {
  //设置输出目录
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession
      .builder()
      .appName("SparkSQL basic example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

//    runBasicDataFrameExample(ss)

//    runBasicDataSetExample(ss)

//    runInferSchemaExample(ss)

    runProgrammaticSchemaExample(ss)

    ss.stop()
  }

  private def runBasicDataFrameExample(ss: SparkSession) = {
    val initDF: DataFrame = ss.read.json("/Users/chenchen/ENN/sparktest/src/main/resources/people.json")
    initDF.show()
    initDF.printSchema()
    initDF.select("age").show()
    import ss.implicits._
    initDF.select($"name",$"age" + 1).show()
    initDF.filter($"age" > 20).show()
    initDF.groupBy("age").count().show()

    initDF.createOrReplaceTempView("person")
    ss.sql("select * from person order by age").show()

    initDF.createGlobalTempView("personInGloble")
    ss.sql("select * from global_temp.personInGloble order by age").show()
    //全局表跨 session
    ss.newSession().sql("select * from global_temp.personInGloble order by age").show()

  }

  private def runBasicDataSetExample(ss: SparkSession) = {
    import ss.implicits._
    //导入隐式转换
    val caseclassDS: Dataset[Person] = Seq(Person("Kobe", 24)).toDS()
    caseclassDS.show()

    val initDS: Dataset[Int] = Seq(1,2,3).toDS()
//    initDS.map(_ + 1).collect().map(println)

    val path = "/Users/chenchen/ENN/sparktest/src/main/resources/people.json"
    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val personDS: Dataset[Person] = ss.read.json(path).as[Person]
    val personDF: DataFrame = personDS.toDF() //DS 转为 DF
    personDS.show()
  }

  private def runInferSchemaExample(ss: SparkSession) = {
    import ss.implicits._
    //converte RDDs to DF
    val personDF: DataFrame = ss.sparkContext
      .textFile("/Users/chenchen/ENN/sparktest/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes=> Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    personDF.show()

    personDF.createOrReplaceTempView("person")
    val sqlResult: DataFrame = ss.sql("select * from person order by age")
    // 对 sql 结果进行抽取
    //By field index
    val teenagerDF: Dataset[String] = sqlResult.map(teenager=> "Name" + teenager(0))
    //By field name
    sqlResult.map(teenager=> "Name" + teenager.getAs[String]("name")).show()
    //No pre-defined encoders for Dataset[Map[K,V]], define explicitly
//    implicitly val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //基本类型与模式匹配也可以这样转换
//    implicitly val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()
//    teenagerDF.map(teenager=> teenager.getValuesMap[Any](List[String, Any])).collect().foreach(println)
  }

  private def runProgrammaticSchemaExample(ss: SparkSession) = {
    import ss.implicits._
    //导入数据源
    val peopleRDD: RDD[String] = ss.sparkContext.textFile("/Users/chenchen/ENN/sparktest/src/main/resources/people.txt")
    //封装 schemaString
    val schemaString = "name age"
    //构建 schema
    val fields = schemaString.split(" ").map(fieldName=> StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //convert records of RDD(people) to Rows
    val rddRows: RDD[Row] = peopleRDD.map(_.split(",")).map(attributes=> Row(attributes(0), attributes(1).trim))
    val peopleDF = ss.createDataFrame(rddRows, schema)
    peopleDF.createOrReplaceTempView("people")
    val resultDF: DataFrame = ss.sql("select * from people order by age")
    //获取某列的值
    resultDF.map(attributes=> "Name:" + attributes(0)).show()
    resultDF.map(attributes=> "Name with field:" + attributes.getAs[String]("name"))

  }

}
