//package enn.enndigit.dealingwithjson
//
//import com.alibaba.fastjson
//import com.alibaba.fastjson.JSON
//import enn.enndigit.Utils.ScalaUtils
//import enn.enndigit.singletoninscala.SparkSessionSingleton
//import kafka.serializer.{Decoder, StringDecoder}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.json.JSONObject
//
//import scala.collection.mutable
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /8/6
//  * @Project:sparktest
//  * @Package:enn.enndigit.dealingwithjson
//  */
////object ParsingJsonFromKafka {
////  val sparkConf: SparkConf = new SparkConf().setAppName("parseJson")
////  val spark: SparkSession = SparkSessionSingleton.getInstance(sparkConf) //引入单例对象
////  val sc: SparkContext = spark.SparkContext //初始化上下文
////  sc.setLogLevel("WARN")
////  val interval = 5 //设置间隔时间
////  val ssc: StreamingContext = new StreamingContext(sc, interval)
////  val schemaMap: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]() //以 Map 存储表结构
////  val schemaMapBroadcast: Broadcast[mutable.Map[String, String]] = sc.broadcast(schemaMap) //将 schemaMap 从转化为广播变量
////  ssc.checkpoint("/Users/chenchen/Desktop/testdata")
////  val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.50.22:9092") //配置 kafka 参数
////  val topicSet = Set("cannal") //配置 topic
////  val lines: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet) //低端消费者拉取kafka数据
////  lines.transform(line => {
////    // json 样例：{database:db1,table:table1,data:{name:sunrui,age:11}}
////    val array: Array[String] = line.map(_._2).collect() //获取 kafka 的值
////    for (x <- line) {
////      val jsonObject: JSONObject = JSON.parseObject(x)
////      val database: String = jsonObject.get("database").toString()
////      val table: String = jsonObject.get("table").toString()
////      val dataValue: String = jsonObject.get("data").toString()
////      val dataJson = ss.sc.makeRDD(Seq(dataValue))
////      val dataFrame: DataFrame = dataJson.read.json(dataJson)
////      dataFrame.createOrReplaceTempView("Json_v")
////      ScalaUtils.getSchema(ss, database, table, schemaMapBroadcast)
////      spark.sql("insert into " + database + "." + table + 5 + "select" + columns + "from Json_v")
////    }
////  })
////  line
////}
//
//
//object ParsingJsonFromKafka {
//  val sparkConf: SparkConf = new SparkConf().setAppName("parseJson")
//  val ss: SparkSession = SparkSessionSingleton.getInstance(sparkConf)
//  val sc: SparkContext = ss.sparkContext
//  sc.setLogLevel("WARN")
//  val interval = 5
//  val ssc: StreamingContext = new StreamingContext(sc, interval)
//  val schemaMap: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]() //以 map 存储表结构
//  val schemaMapBroadcast: Broadcast[mutable.Map[String, String]] = sc.broadcast(schemaMap)
//  ssc.checkpoint("/#")
//  val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.1.1:9092")
//  val topicSet = Set("cannal")
//  val lines: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)
////    {database:db1,table:table1,data:{name:sunrui,age:11}}
//  lines.transform(line=> {
//    val array: Array[String] = line.map(_._2).collect()
//    for (x <- line) {
//      val jsonObject: JSONObject = JSON.parseObject(x)
//      val database: String = jsonObject.get("database").toString
//      val table: String = jsonObject.get("table").toString
//      val dataValue: String = jsonObject.get("data").toString
//      val dataJson = ss.sc.makeRDD(Seq(dataValue))
//      val dataFrame: DataFrame = dataJson.read.json(dataJson)
//      dataFrame.createOrReplaceTempView("Json_v")
//      ScalaUtils.getSchema(ss, database, table, schemaMapBroadcast)
//      ss.sql("insert into " + database + "." + table + 5 + "select" + columns + "from Json_v")
//    }
//  })
//  line
//}
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
