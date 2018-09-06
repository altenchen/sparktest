//package enn.enndigit.iplocation
//
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/13
//  * @Project:sparktest
//  * @Package:enn.enndigit.iplocation
//  */
//object Iplocations {
//
//  //iplcoation 计算
//  def main(args: Array[String]): Unit = {
//    //程序启动入口校验
//    if (args.length != 2) {
//      System.err.print("Parameters were not enough, please check and restart!")
//      System.exit(1)
//    }
//
//    //转换ip 数据类型
//    def ip2Long(ips: String): Long = {
//      val ipsFormat: Array[String] = ips.split("\\.")
//      var ipNum: Long = 0L
//      for (i <- ipsFormat) {
//        ipNum = i.toLong | ipNum << 8L
//      }
//      ipNum
//    }
//
//    //在经纬度中查找ip 位置
//    def binarySearch(ipNum: Int, broadcastArray: Broadcast[Array[(String, String, String, String)]]): Int = {
//      var low = 0
//      var high: Int = broadcastArray.length - 1
//      while (low <= high) {
//        val middle = (low + high) / 2
//        if (ipNum >= broadcastArray(middle)._1.toLong && ipNum <= broadcastArray(middle)._2.toLong){
//          return ipNum
//        } else if (ipNum < broadcastArray(middle)._1.toLong) {
//          high = middle -1
//        } else (ipNum > broadcastArray(middle)._2.toLong) {
//          low = middle + 1
//        }
//      }
//      return -1
//    }
//
//    //city_ips 信息获取
//    val cityIps: String = args(0)
//    //userIps 信息获取
//    val userIps: String = args(1)
//    //1.获取spark上下文对象
//    val conf: SparkConf = new SparkConf().setAppName("IplocationCounting").setMaster("local[2]")
//    val sc: SparkContext = new SparkContext(conf)
//    //2.数据格式化整理
//    val city_ips: RDD[String] = sc.textFile(cityIps)
//    //3.获取基站 RDD
//    val jizhanRDD: RDD[(String, String, String, String)] = city_ips.map(_.split("\\|")).map(x=> (x(2), x(3), x(12), x(13)))
//    //4.将基站RDD广播到 worker 节点
//    val broadcastCityIps: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(jizhanRDD.collect())
//    //5.获取 user 的 ip 信息
//    val user_ips: RDD[String] = sc.textFile(userIps)
//    val userIp: RDD[String] = user_ips.map(_.split("\\|")).map(x=> x(1))
//    //获取userIp 在cityIp范围内出现的次数 并做统计
//    val result: RDD[((String, String), Int)] = userIp.mapPartitions(iter=> {
//      val broadcastValue: Array[(String, String, String, String)] = broadcastCityIps.value
//      iter.map(ip=> {
//        val ipNum = ip2Long(ip)
//        val index = binarySearch(ipNum, broadcastValue)
//      })
//      ((broadcastValue._3, broadcastValue._4), index)
//    })
//    //对指定city_ips中user_ip出现的次数进行统计
//    val finalResult: RDD[((String, String), Int)] = result.reduceByKey(_+_).sortByKey(true)
//    finalResult.foreach(print)
//
//  }
//}
