package enn.enndigit.toplink

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/8
  * @Project:sparktest
  * @Package:enn.enndigit.toplink
  */
object SocketStreaming extends App {
  val conf: SparkConf = new SparkConf().setAppName("NetWorkWordCount").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(5))

  val sourceData: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

  val words: DStream[String] = sourceData.flatMap(_.split(" "))

  val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

  val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_+_)

//  def updateFunction(newValues:Seq[Int], runningCount: Option[Int]): Option[Int] = {
//    val newCount = Seq(wordCounts)
//    Some(newCount)
//  }
//
//  val result: DStream[(String, Int)] = pairs.updateStateByKey[Int](updateFunction _)

//  wordCounts.foreachRDD { rdd =>
//    rdd.foreachPartition { partitionOfRecords =>
//      // ConnectionPool is a static, lazily initialized pool of connections
//      val connection = ConnectionPool.getConnection()
//      partitionOfRecords.foreach(record => connection.send(record))
//      ConnectionPool.returnConnection(connection)  // return to the pool for future reuse
//    }
//  }

//  wordCounts.foreachRDD { rdd =>
//    rdd.foreachPartition { partitionOfRecords =>
//      val connection = createNewConnection()
//      partitionOfRecords.foreach(record => connection.send(record))
//      connection.close()
//    }
//  }


  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()

}
