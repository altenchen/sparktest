package enn.enndigit.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/4
  * @Project:sparktest
  * @Package:enn.enndigit.structuredstreaming
  */
object HandleWordCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession
      .builder()
      .appName("StructuredStreaming Example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    handleWordCount(ss)

  }

  def handleWordCount(ss: SparkSession) = {
    val initStream: DataFrame = ss.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    println(initStream.isStreaming)

    initStream.printSchema()
    import ss.implicits._
    val lines: Dataset[String] = initStream.as[String].flatMap(_.split(" "))
    val wordcount: DataFrame = lines.groupBy("value").count()

    val query = wordcount
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
