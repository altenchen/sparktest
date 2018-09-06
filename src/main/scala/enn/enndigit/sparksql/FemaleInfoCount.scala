//package enn.enndigit.sparksql
//
//
//import java.util
//
//import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql._
//import org.slf4j
//import org.slf4j.LoggerFactory
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/29
//  * @Project:sparktest
//  * @Package:enn.enndigit.sparksql
//  */
//case class FemaleInfo(name: String, gender: String, stayTime: Long)
//
//object FemaleInfoCount {
//
////  private val util = new PropertiesUtil("/Users/chenchen/ENN/sparktest/src/main/resources/dev/common.propertites")
//
//  private val log: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
//  val conf: SparkConf = new SparkConf().setAppName("femaleWithTatolCount").setMaster("local[2]")
//  val sc = new SparkContext(conf)
//  sc.setLogLevel("WARN")
//  val ss: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//
//  def main(args: Array[String]): Unit = {
//
////    showJson(ss)
////
////    parseFemaleInfo(sc)
//
//    jsonClean(sc)
//    sc.stop()
//
//  }
//
//  /*
//    "allPoints":300,
//    "data":Array[300],
//    "domain":"UES",
//    "staId":"CA30ES03",
//    "version":"V01"
//
//    "data":[
//            {
//                "metric":"SVAL_MV1022_ALMcloseAct",
//                "time":1534754730195,
//                "value":0
//            },
//            {
//                "metric":"SVAL_MV1022_ALMcloseAct",
//                "time":1534754730195,
//                "value":0
//            }]
//
//   */
//
//  //parsingJson in fastJson
//  def jsonClean(sc: SparkContext): RDD[String] = {
//    //读取数据源
//    val list = scala.collection.mutable.MutableList[String]()
//    var result: ArrayBuffer[String] = new ArrayBuffer[String]()
//    val initJson: RDD[String] = sc.textFile("/Users/chenchen/MRS/kafka_json.txt")
//
//    initJson.foreach(println)
//    //val initalRDD: DataFrame = ss.read.json("/Users/chenchen/MRS/kafka_json.txt")
//    //解析数据源
//
//    val resultRDD =
//      initJson.map(jsonData=> {
//      //解析 json 字符串
//      println("-------jsonData-------")
//      println(jsonData)
//        jsonData.foreach(println)
//      val jsonObj: JSONObject = JSON.parseObject(jsonData)
//      println("-------jsonObj--------")
//      println(jsonObj)
//      //获取 json 的各个一级 key
//      val allPoints = jsonObj.getString("allPoints")
//      val domainMK = jsonObj.getString("domain")
//      val staId = jsonObj.getString("staId")
//      val version = jsonObj.getString("version")
//      val jsonArray: JSONArray = jsonObj.getJSONArray("data")
//      val iter = jsonArray.iterator()
//      while (iter.hasNext()) {
//        val inner = iter.next().asInstanceOf[JSONObject]
//        val metric = inner.getString("metric")
//        if (!metric.trim.isEmpty()) {
//          try {
//            val timestamp = inner.getString("time")
//
//            val value = inner.getString("value")
//            val splits = metric.split("_")
//            if (splits.length > 0) {
//              var tagStr = ""
//              val tag1 = inner.get("tag1")
//              if (tag1 != null) {
//                tagStr = tagStr + "\"tag1\":\"" + tag1 + "\","
//              }
//              val tag2 = inner.get("tag2")
//              if (tag2 != null) {
//                tagStr = tagStr + "\"tag2\":\"" + tag2 + "\","
//              }
//              val tag3 = inner.get("tag3")
//              if (tag3 != null) {
//                tagStr = tagStr + "\"tag3\":\"" + tag3 + "\","
//              }
//              val tag4 = inner.get("tag4")
//              if (tag4 != null) {
//                tagStr = tagStr + "\"tag4\":\"" + tag4 + "\","
//              }
//              val tag5 = inner.get("tag5")
//              if (tag5 != null) {
//                tagStr = tagStr + "\"tag5\":\"" + tag5 + "\","
//              }
//              val tag6 = inner.get("tag6")
//              if (tag6 != null) {
//                tagStr = tagStr + "\"tag6\":\"" + tag6 + "\","
//              }
//
//              val equipMK = splits(0)
//              val equipID = splits(1)
//              val AttriMK = splits(2)
//
//              var returnStr = ""
//              if (!tagStr.equals("")) {
//                returnStr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",tags:{\"staId\":\"" +
//                  staId + "\",\"equipMK\":\"" + equipMK + "\",\"equipID:\"" + equipID + "\"}}"
//              } else {
//                returnStr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",tags:{\"staId\":\"" +
//                  staId + "\",\"equipMK\":\"" + equipMK + "\",\"equipID:\"" + equipID + "\"," + returnStr.substring(0, returnStr.length - 1) + "\"}}"
//              }
//              try {
//                if (!value.toString().contains("NaN")) {
//                  result += returnStr
//                } else {
//                  log.error("数据采集异常：" + value)
//                }
//              } catch {
//                case e: Exception =>
//                  log.error("数据异常为空")
//              }
//            }
//          } catch {
//            case e: Exception =>
//              log.error("解析 json 数据异常")
//          }
//        }
//      }
//        result
//        var resultBuffer:ArrayBuffer[String] =  new ArrayBuffer[String]()
//        val value: ArrayBuffer[String] = result.foreach(x=> {
//          value += x
//        })
//        value
//    }
//    )
//    result.toArray.foreach(println)
////    println("打印 Array :" + result)
//    sc.parallelize(result)
//  }
//
//  import ss.implicits._
//  def showJson(ss: SparkSession): Unit = {
//    //读取 json 数据
//    val jsonData: DataFrame = ss.read.json("/Users/chenchen/MRS/kafka_json.txt")
//    jsonData.createOrReplaceTempView("jsonTable")
//    val resultData: DataFrame = ss.sql("select count(data),domain from jsonTable group by domain")
//    val frame: Dataset[Row] = ss.sql("select count(data),domain domain01 from jsonTable group by domain")
//    resultData.write.mode(SaveMode.Append).sortBy().saveAsTable()
//
//    log.info("printing jsonInfo......")
//    resultData.show()
//  }
//
//
//  import ss.implicits._
//  def parseFemaleInfo(sc: SparkContext): Unit = {
//    sc.textFile("/Users/chenchen/Desktop/testdata/femaleinfo.txt").map(_.split(",")).map(elem => FemaleInfo(elem(0), elem(1), elem(2).trim.toInt))
//      .toDF.createOrReplaceTempView("femaletable")
//    val femaleInfo: DataFrame = ss.sql("select name,sum(stayTime) stayTime from femaletable where gender='female' group by name")
//    log.info("printing femaleInfo......")
//    femaleInfo.filter(x => x.getAs[Long]("stayTime") > 120).collect().foreach(println)
////    femaleInfo.filter("stayTime >= 120").collect().foreach(println)
//  }
//}
