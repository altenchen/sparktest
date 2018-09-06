package enn.enndigit.clickstream

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/6
  * @Project:sparktest
  * @Package:enn.enndigit.clickstream
  */
object StreamingSocketTest {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Params Not Enough")
      System.exit(1)
    }

    val host: String = args(0)
    val port: Int = args(1).toInt

    StreamingLogger.setStreamingLogger()
    val conf: SparkConf = new SparkConf().setAppName("SocketStreaming Test").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val interval: Duration = Seconds(10)
    val ssc: StreamingContext = new StreamingContext(sc,interval)

    val socketStreming: DStream[String] = ssc.socketTextStream(host, port).flatMap(_.split("\n"))

    socketStreming.foreachRDD(rdd=> {
      rdd.foreach(x=> {
        x.foreach(print)
        println()
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }

}
