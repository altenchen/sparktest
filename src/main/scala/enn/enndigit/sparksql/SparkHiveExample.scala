package enn.enndigit.sparksql

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/3
  * @Project:sparktest
  * @Package:enn.enndigit.sparksql
  */

case class Records(key: Int, value: String)
object SparkHiveExample {
    Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    //1、create path of warehouselocation
    val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath
    //2、init ss
    val ss: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //3、create table and load source data，创建 Hive表
    import ss.implicits._
    import ss.sql
    ss.sql("create table if not exists inittable(key Int, value String) using Hive")
    //将数据导入到 hive 表中
    ss.sql("load data local inpath '/Users/chenchen/ENN/sparktest/src/main/resources/kv1.txt' into table inittable")
    //4、start query
    ss.sql("select * from inittable").show()
    val sqlDF: DataFrame = ss.sql("select key, value from inittable where key < 10 order by key")
    //The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val StringDS: Dataset[String] = sqlDF.map {
      case Row(key: Int, value: String) => s"key:$key, value:$value"
    }
    StringDS.show()

    val tempDF: DataFrame = ss.createDataFrame((1 to 100).map(i=> Records(i, s"val_$i")))
    tempDF.show()
    tempDF.createOrReplaceTempView("DFtable")
    ss.sql("select distinct * from DFtable d join inittable i on d.key=i.key order by d.key").show()
    ss.stop()

  }
}
