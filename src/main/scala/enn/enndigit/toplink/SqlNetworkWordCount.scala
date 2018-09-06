package enn.enndigit.toplink

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.slf4j.LoggerFactory

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/8
  * @Project:sparktest
  * @Package:enn.enndigit.toplink
  */
object SqlNetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: SqlNetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    val sc1 = new SparkContext(conf)
    val sc2 = new SparkContext(conf)

    sc1.setLogLevel("WARN")
    sc2.setLogLevel("WARN")
    val ssc1: StreamingContext = new StreamingContext(sc1, Seconds(5))
    val ssc2: StreamingContext = new StreamingContext(sc2, Seconds(5))

    val lines1: ReceiverInputDStream[String] = ssc1.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK)
    val lines2: ReceiverInputDStream[String] = ssc2.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK)
    val words1: DStream[String] = lines1.flatMap(_.split(" "))
    val words2: DStream[String] = lines2.flatMap(_.split(" "))

    //将 dataframe 的 rdd 转换为 case class 的 rdd
    words1.foreachRDD((rdd: RDD[String], time: Time)=> {
      val ss: SparkSession = SingletonSparkSession.getInstance(rdd.sparkContext.getConf)
      import ss.implicits._
      val wordsDataFrame: DataFrame = rdd.map(x=> Record(x)).toDF()
      wordsDataFrame.createOrReplaceTempView("words")
      val wordcountDadaFrame: DataFrame = ss.sql("select word, count(*) as total from words group by word")
      println(s"=========$time==========thread1")
      wordcountDadaFrame.show()
    })

    words2.foreachRDD((rdd: RDD[String], time: Time)=> {
      val ss: SparkSession = SingletonSparkSession.getInstance(rdd.sparkContext.getConf)
      import ss.implicits._
      val wordsDataFrame: DataFrame = rdd.map(x=> Record(x)).toDF()
      wordsDataFrame.createOrReplaceTempView("words")
      val wordcountDadaFrame: DataFrame = ss.sql("select word, count(*) as total from words group by word")
      println(s"=========$time==========thread2")
      wordcountDadaFrame.show()
    })


    ssc1.start()
    ssc2.start()
    ssc1.awaitTermination()
    ssc2.awaitTermination()

  }
}

case class Record(word: String)

object SingletonSparkSession{
  //创建单例对象
  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
