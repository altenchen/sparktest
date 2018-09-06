import org.apache.spark.sql.{DataFrame, SparkSession}

val ss = SparkSession
  .builder()
  .appName("sparkSQL basic example demo")
  .config("spark.some.config.option", "some-value")
  .master("local[2]")
  .getOrCreate()

val df: DataFrame = ss.read.json("/Users/chenchen/Desktop/testdata/testjson.json")
df.show()
