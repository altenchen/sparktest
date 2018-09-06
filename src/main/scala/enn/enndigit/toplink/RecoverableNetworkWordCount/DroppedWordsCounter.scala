package enn.enndigit.toplink.RecoverableNetworkWordCount

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/8
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.RecoverableNetworkWordCount
  */
object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}
