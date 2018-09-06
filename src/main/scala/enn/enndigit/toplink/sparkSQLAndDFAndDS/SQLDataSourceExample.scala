package enn.enndigit.toplink.sparkSQLAndDFAndDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/9
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.sparkSQLAndDFAndDS
  */

object SQLDataSourceExample {



  def main(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession
      .builder()
      .appName("SQLDataSourceExample")
      .master("local[2]")
      .getOrCreate()

    ss.sparkContext.setLogLevel("WARN")

//    runBasicDataSourceExample(ss)

//    runBasicParquetExample(ss)

    runJsonDatasetExample(ss)




  }




  def runBasicDataSourceExample(ss: SparkSession) = {
    val userDF: DataFrame = ss.read.load("/Users/chenchen/Desktop/testdata/sparksqldata/users.parquet")

    userDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    val peopleDF: DataFrame = ss.read.format("json").load("/Users/chenchen/Desktop/testdata/sparksqldata/people.json")

    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

    val sqlDF: DataFrame = ss.sql("select * from parquet.`/Users/chenchen/Desktop/testdata/sparksqldata/users.parquet`")
  }


  private def runBasicParquetExample(ss: SparkSession) = {
    import ss.implicits._
    val peopleRDD: DataFrame = ss.read.json("/Users/chenchen/Desktop/testdata/sparksqldata/people.json")
    peopleRDD.write.parquet("people.parquet")
    val parquetFileDF: DataFrame = ss.read.parquet("people.parquet")

    parquetFileDF.createOrReplaceTempView("parquetFileDF")

    val nameDF: DataFrame = ss.sql("select name from parquetFileDF where age between 10 and 20")
    nameDF.map(attribute=> "Name:" + attribute.getAs[String]("name")).show()

  }

  private def runJsonDatasetExample(ss: SparkSession) = {

    val peopleDF: DataFrame = ss.read.json("/Users/chenchen/Desktop/testdata/sparksqldata/people.json")

    peopleDF.createOrReplaceTempView("people")

    peopleDF.printSchema()

//    ss.sql("select name, age from people group by age").show()
    ss.sql("select name from people where age between 10 and 20").show()

    val otherPeopleRDD: RDD[String] = ss.sparkContext.makeRDD(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil
    )

    val otherPeople: DataFrame = ss.read.json(otherPeopleRDD)

    otherPeople.createOrReplaceTempView("otherPeople")

    ss.sql("select * from otherPeople").show()

    otherPeople.show()


  }








}
