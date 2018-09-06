package enn.enndigit.toplink.sparkSQLAndDFAndDS

import org.apache.spark.sql.SparkSession

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/10
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.sparkSQLAndDFAndDS
  */


case class Record(key: Int, value: String)

object SparkHiveExample {

  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  def main(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    import ss.implicits._
    import ss.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH '/Users/chenchen/Desktop/testdata/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()


//    ss.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    ss.sql("LOAD DATA LOCAL INPATH '/Users/chenchen/Desktop/testdata/kv1.txt' INTO TABLE src")
//    ss.sql("select * from src").show()







  }
}
