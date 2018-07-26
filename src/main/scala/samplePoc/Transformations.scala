package samplePoc


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Transformations {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Transformations")
      .config("spark.driver.bindAddress","127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val file = spark.read.textFile("src/main/resources/poc_data/textFiles/sample1.txt")
    val words = file.rdd.flatMap(_.split(" "))
    val wordsPairs = words.map((_,1))


    //finding the word count
    val wordCount = wordsPairs.reduceByKey(_+_)

    //grouping by key
    val wordPairsGrouped = wordsPairs.groupByKey()

    //aggregating by key
    val wordPairAggregated =wordsPairs.aggregateByKey()



  }

}
