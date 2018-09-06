package enn.enndigit.singletoninscala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/6
  * @Project:sparktest
  * @Package:enn.enndigit.singletoninscala
  */
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance == SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}
