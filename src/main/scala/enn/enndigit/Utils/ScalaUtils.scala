package enn.enndigit.Utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /8/6
  * @Project:sparktest
  * @Package:enn.enndigit.Utils
  */
object ScalaUtils {
  def getSchema(ss: SparkSession, database: String, tableName: String, schemaMapBroadcast: Broadcast[mutable.Map[String,String]]): mutable.Map[String,String] = {
    val schemaMap: mutable.Map[String, String] = schemaMapBroadcast.value
    val Remap: mutable.Map[String, String] = mutable.Map[String, String]()
    var columns = ""
    var size = ""
    if (schemaMap.contains(tableName)) {
      columns = schemaMap.get(tableName).getOrElse("")
      size = schemaMap.get(tableName + "size").getOrElse("")
    } else {
      val map = getSchemaFromHive(ss, database, tableName)
      columns = map.get("columnData").getOrElse("")
      size = map.get("columnSize").getOrElse("0")
      schemaMap += (tableName -> columns)
      schemaMap += (tableName + "size" -> size)
    }
    Remap += ("columnData" -> columns)
    Remap += ("columnSize" -> size)
    Remap
  }

  def getSchemaFromHive(ss: SparkSession, database: String, tableName: String) : mutable.Map[String, String] = {
    val map = mutable.Map[String, String]()
    val sql = "desc " + database + "." +tableName
    val schema = ss.sql(sql)
    val array = schema.rdd.collect()
    val stringBF = new StringBuffer()
    val list = new ListBuffer[String]()
    array.foreach(x=> {
      list += x(0).toString()
    })
    val last = list.toList.last
    val columnSize = list.size
    for (i <- list){
      if (!last.equals(i)){
        stringBF.append(i + ",")
      } else {
        stringBF.append(i)
      }
    }
    map += ("columnSize" -> columnSize.toString)
    map += ("columnData" -> stringBF.toString)
    map
  }
}
