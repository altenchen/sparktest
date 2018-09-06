package enn.enndigit.structuredstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/4
  * @Project:sparktest
  * @Package:enn.enndigit.structuredstreaming
  */
object HandleCsvFile {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss: SparkSession = SparkSession
      .builder()
      .appName("HandleCsvFile Example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    handleCsvFile(ss)

  }


  def handleCsvFile(ss: SparkSession) = {

    val csvSchema = new StructType().add("name", "string").add("age", "integer")

    val initDF: DataFrame = ss.readStream
      .format("csv")
      .option("sep", ";")
      .schema(csvSchema)
//      .csv("/Users/chenchen/ENN/sparktest/src/main/resources/csvSchema.csv")
      .format("csv")
      .load("/Users/chenchen/ENN/sparktest/src/main/resources/csvSchema.csv")

//    val query: StreamingQuery = initDF
//      .writeStream
//      .outputMode("complete")
//      .format("console")
//      .start()
//    query.awaitTermination()
  }
}
