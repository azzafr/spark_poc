package samplePoc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Poc1 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("poc1")
      .getOrCreate()

    import spark.implicits._

    val sample1: Dataset[String] = spark.read.textFile("src/main/resources/poc_data/textFiles/sample1.txt")
    //println("sample1 count : "+sample1.count())
    //println(sample1.first())

    //filter
    val sample1Filtered = sample1.filter( line => line.contains("word"))
    //println("sample1Filtered count : "+sample1Filtered.count())
    //sample1Filtered.rdd.foreach(println)


    //we have Dataset[String], such that we will find size of each line and create Dataset[Int]
    val lineSizeArray: Dataset[Int] = sample1Filtered.map(line => line.size)
    //lineSizeArray.show()


    val rdd1: RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4,5))
    //rdd1.foreach(println)
    println("sum of rdd1 : "+rdd1.reduce((a,b) =>a+b))






  }

}
