package enn.enndigit.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/28
  * @Project:sparktest
  * @Package:enn.enndigit.sparkstreaming
  */
object CountJson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("jsonCount").setMaster("local[2]")
    val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val dataFrame: DataFrame = ss.read.json("/Users/chenchen/Desktop/testdata/countjson.txt")
    dataFrame.createGlobalTempView("jsontable")
    val resultData: DataFrame = ss.sql("select pid,count(pid) as total from jsontable group by pid")
    resultData.rdd.cache().saveAsTextFile("/Users/chenchen/Desktop/testdata/output/")
  }
}
