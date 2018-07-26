package samplePoc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.bindAddress","127.0.0.1")
      .appName("WordCount")
      .getOrCreate()

    //using reduceByKey operator

    val file = spark.read.textFile("src/main/resources/poc_data/textFiles/sample1.txt")

    val data = file.rdd.flatMap(line => line.split(" "))
    val dataPair = data.map((_,1))
    val wordCount = dataPair.reduceByKey(_+_)
    wordCount.take(10).foreach(println)



  }

}
