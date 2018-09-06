package enn.enndigit.clickstream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.clickstream
  */
object PageViewStream {
  def main(args: Array[String]): Unit = {
    //设置日志
    StreamingLogger.setStreamingLogger()
    //入参格式判定
    if (args.length != 3) {
      System.err.println("Params Not Enough")
      System.exit(1)
    }

    //main 方法入参 metric host port
    val metric = args(0)
    val host = args(1)
    val port = args(2).toInt

    //初始化 ssc
//    val ssc = new StreamingContext("local[2]", "PageViewStream", Seconds(1), System.getenv("SPARK_HOME"),
//      StreamingContext.jarOfClass(this.getClass).toSeq)
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("PageViewStream").setMaster("local[2]"))
    val interval = Seconds(5)
    val ssc: StreamingContext = new StreamingContext(sc, interval)

    //按格式获取封装好的 pageView
    val pageViews: DStream[PageView] = ssc.socketTextStream(host, port)
      .flatMap(_.split("\n"))
      .map(PageView.fromString(_))

    //统计每个批次的 URL
    val pageCounts = pageViews.map(view=> view.url).countByValue()

    //窗口化 PageCounts，十分钟内数据 没两秒展示展示一次
    val slidingPageCounts = pageViews.map(view=> view.url)
      .countByValueAndWindow(Seconds(10), Seconds(2))

    //获取过去三十秒内的异常（状态返回不为200）访问率
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(2))
      .map(view => (view.zipeCode, view.status))
      .groupByKey()

    val errorRatePerZipCode: DStream[String] = statusesPerZipCode.map {
      case (zip, statuses) => {
        val normalStatus: Any = statuses.count(_ == 200)
        val errorStatus = statuses.size - normalStatus
        val errorRatio = errorRatio.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s, **%S**".format(zip, errorRatio)
        } else {
          "%s, %s".format(zip, errorRatio)
        }
      }
    }

    //过去15秒的 userID
    val activeUserCount: DStream[String] = pageViews.window(Seconds(15), Seconds(2))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("the unique userID in last 15s" + _)

    //向 userList加入外部数据源
    val userList: RDD[(Int, String)] = ssc.sparkContext.parallelize(Seq(
      1 -> "Patrick Wendell",
      2 -> "Reynold Xin",
      3 -> "Matei Zaharia"
    ))

    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "statusesPerZipCode" => statusesPerZipCode.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
        pageViews.map(view=> (view.userID, 1))
          .foreachRDD((rdd, time)=> rdd.join(userList)
            .map(_._2._2)
              .take(10)
                .foreach(u=> println("saw user %s at time %s".format(u, time)))
          )
      case _ => println("Invalid metric parameter" + metric)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
