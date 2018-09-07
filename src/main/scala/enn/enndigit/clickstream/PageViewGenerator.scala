package enn.enndigit.clickstream

import java.io.PrintWriter
import java.net.{ServerSocket, Socket}
import java.util.Random

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.clickstream
  */

//点击流日志详细信息：url, status, zipCode, userID
//主构造方法
class PageView(val url: String, val status: Int, val zipCode: Int, val userID: Int) extends Serializable {
  override def toString: String = {
    "%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID)
  }
}

//构建点击流日志伴生对象 PageView
object PageView extends Serializable {
  def fromString(in: String): PageView = {
    val parts: Array[String] = in.split("\t")
    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt)
  }
}


object PageViewGenerator {

  //点击流日志需统计信息包括： pages, httpStatus, userZipCode, userID
  //页面访问
  val pages: Map[String, Double] = Map(
    "http://foo.com/" -> .7,
    "http://foo.com/news" -> 0.2,
    "http://foo.com/contact" -> .1
  )
  //http状态
  val httpStatus: Map[Int, Double] = Map(
    200 -> .95,
    404 -> .05
  )
  //zip编码
  val zipCode = Map(
    94709 -> .5,
    94709 -> .5
  )
  //userID
  val userID = Map((1 to 100).map(_ -> .01): _*)

  //获取指标
  def pickFromDistribution[T](inputMap: Map[T, Double]): T = {
    var rand = new Random().nextDouble()
    var total = 0.0
    for ((item, prop) <- inputMap) {
      total = prop + total
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1
  }

  //获取点击流日志
  def getNextClickEvent(): String = {
    val page: String = pickFromDistribution(pages)
    val status: Int = pickFromDistribution(httpStatus)
    val code: Int = pickFromDistribution(zipCode)
    val id: Int = pickFromDistribution(userID)
    new PageView(page, status, code, id).toString()
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: PageViewGenerator <port> <viewsPerSecond>")
      System.exit(1)
    }

    //传入参数： port，viewPerSecond
    val port = args(0).toInt
    val viewPerSecond = args(1).toFloat
    val sleepDelayMs = (1000 / viewPerSecond).toInt
    val listener: ServerSocket = new ServerSocket(port)
    println("Listening on port:" + port)

    while (true) {
      val socket: Socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("got client coonected from: " + socket.getInetAddress)
          val out: PrintWriter = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(sleepDelayMs)
            out.write(getNextClickEvent())
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
