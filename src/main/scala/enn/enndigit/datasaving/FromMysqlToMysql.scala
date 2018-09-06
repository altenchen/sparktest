//package enn.enndigit.datasaving
//
//import java.util.Properties
//
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/7
//  * @Project:sparktest
//  * @Package:enn.enndigit.datasaving
//  */
//case class cc_test(longitude:String, latitude:String,total_count:Int)
//
//object FromMysqlToMysql {
//
//
//    def main(args: Array[String]) {
//      val conf = new SparkConf().setAppName("mysql").setMaster("local[4]")
//      val ss: SparkSession = SparkSession(conf)
//      val sc: SparkContext = ss.sparkContext
//
//      //定义mysql信息
//      val jdbcDF = ss.read.format("jdbc").options(
//        Map("url"->"jdbc:mysql://localhost:3306/spark",
//          "dbtable"->"(select longitude,latitude,total_count from iplocation) as some_alias",
//          "driver"->"com.mysql.jdbc.Driver",
//          "user"-> "root",
//          //"partitionColumn"->"day_id",
//          "lowerBound"->"0",
//          "upperBound"-> "1000",
//          //"numPartitions"->"2",
//          "fetchSize"->"100",
//          "password"->"123456")).load()
//      jdbcDF.collect().take(20).foreach(println) //终端打印DF中的数据。
//      //jdbcDF.rdd.saveAsTextFile("C:/Users/zhoubh/Downloads/abi_sum")
//      val url="jdbc:mysql://localhost:3306/spark"
//      val prop=new Properties()
//      prop.setProperty("user","root")
//      prop.setProperty("password","123456")
//      jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,"iplocation",prop) //写入数据库db_ldjs的表 zfs_test 中
//      //jdbcDF.write.mode(SaveMode.Append).jdbc(url,"zbh_test",prop)  //你会发现SaveMode改成Append依然无济于事，表依然会被重建，为了解决这个问题，后期会另开博客讲解
//      //org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(jdbcDF,url,"zbh_test",prop)
//      ////    #然后进行groupby 操作,获取数据集合
//      //    val abi_sum_area = abi_sum.groupBy("date_time", "area_name")
//      //
//      ////    #计算数目，并根据数目进行降序排序
//      //    val sorted = abi_sum_area.count().orderBy("count")
//      //
//      ////    #显示前10条
//      //    sorted.show(10)
//      //
//      ////    #存储到文件（这里会有很多分片文件。。。）
//      //    sorted.rdd.saveAsTextFile("C:/Users/zhoubh/Downloads/sparktest/flight_top")
//      //
//      //
//      ////    #存储到mysql表里
//      //    //sorted.write.jdbc(url,"table_name",prop)
//    }
//
//  }
