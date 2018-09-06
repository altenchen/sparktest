package enn.enndigit.clickstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /9/5
  * @Project:sparktest
  * @Package:enn.enndigit.clickstream
  */
object StreamingLogger extends Logging{
    def setStreamingLogger() = {
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        logInfo("set LogLevel to ERROR")
        Logger.getRootLogger.setLevel(Level.ERROR)
      }
    }
}
