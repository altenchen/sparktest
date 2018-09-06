package enn.enndigit.toplink.RecoverableNetworkWordCount

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.{IntParam, LongAccumulator}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/8
  * @Project:sparktest
  * @Package:enn.enndigit.toplink.RecoverableNetworkWordCount
  */
object RecoverableNetworkWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.print(s"your arguments were ${args.mkString("[", ", ", "]")}")
      System.err.println(
        """
          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
          |     word counts will be appended
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |Both <checkpoint-directory> and <output-file> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }

    val Array(ip, port, checkpointDirectory, outputPath) = args
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, ()=> createContext(ip, port.toInt, outputPath, checkpointDirectory))


    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(ip: String, port: Int, outputPath: String, checkpointDirectory: String): StreamingContext = {
    println("Creating new context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val conf: SparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(ip, port)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordcounts: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_ + _)

    wordcounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val blacklist: Broadcast[Seq[String]] = WordBlacklist.getInstance(rdd.sparkContext)
      val droppedWordsCounter: LongAccumulator = DroppedWordsCounter.getInstance(rdd.sparkContext)

      val counts: String = rdd.filter { case (word, count) => {
        if (blacklist.value.contains(word)) {
          droppedWordsCounter.add(count)
          false
        } else {
          true
        }
      }
      }.collect().mkString("[", ", ", "]")

      val output = s"Counts at time $time $counts"
      println(output)
      println(s"Dropped ${droppedWordsCounter.value} word(s) totally")
      println(s"appending to ${outputFile.getAbsolutePath}")
//      Files.append(output + "\n", outputFile, Charset.defaultCharset())
//      Files.append(output + "\n", outputFile, Charset.defaultCharset())
      Files.append(output + "\n", outputFile, Charset.defaultCharset())
    })
    ssc
  }
}


/*
spark-submit --class enn.enndigit.toplink.RecoverableNetworkWordCount.RecoverableNetworkWordCount --master yarn-client --executor-memory  1g --num-executors  5  --driver-memory 1g --jars /export/data/spark/streamingcount.jar localhost 9999 /user/test/networdcount  /user/test/networdcountdata --name spark_cc
 */


