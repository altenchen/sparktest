//package enn.enndigit.dealingwithjson
//import java.util
//
//import net.minidev.json.JSONObject
//import net.minidev.json.parser.JSONParser
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/6
//  * @Project:sparktest
//  * @Package:enn.enndigit.dealingwithjson
//  */
//object ParsingJsonWithSmartJson {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("parsejson02").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val str2 = "{\"name\":\"jeemy\",\"age\":25,\"phone\":\"18810919225\"}"
//    sc.textFile("")
//    val jsonParser: JSONParser = new JSONParser(3)
//
//    val jsonObj: JSONObject = jsonParser.parse(str2).asInstanceOf[JSONObject]
//    val name: String = jsonObj.get("name").toString
//    println(name)
//    val jsonKey: util.Set[String] = jsonObj.keySet()
//    val iter: util.Iterator[String] = jsonKey.iterator
//    while (iter.hasNext) {
//      val key: String = iter.next()
//      val value: String = jsonObj.get(key).toString
//      println("key: " + key + " value: " + value)
//    }
//  }
//}
