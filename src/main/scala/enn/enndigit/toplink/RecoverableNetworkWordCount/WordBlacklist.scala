package enn.enndigit.toplink.RecoverableNetworkWordCount

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/8
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.RecoverableNetworkWordCount
  */
object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("dianliu", "dianya", "dianzu")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}
