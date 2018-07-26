package samplePoc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

class PassingFuntions extends  Serializable {

  def calculatLen(s:String) :Int = s.length

}

class StringOperations extends Serializable{
  def toUpperCase(s:String):String=s.toUpperCase
  def toLowerCase(s:String):String=s.toLowerCase
}

object PassingFuntionsObject {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PassingFuntionsObject")
      .config("spark.driver.bindAddress","127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val file = spark.read.textFile("src/main/resources/poc_data/textFiles/sample1.txt")
    val pf = new PassingFuntions()

    val fileComputed: RDD[Int] =  file.rdd.map(line => pf.calculatLen(line))
    //fileComputed.foreach(println)

    val so = new StringOperations
    val fileUpperCase: RDD[String] = file.rdd.map(so.toUpperCase)
    val fileLowerCase: RDD[String] = file.rdd.map(so.toLowerCase)

    fileLowerCase.foreach(println)
    println("--------------------------")
    fileUpperCase.foreach(println)









    //fileComputed.foreach(println)



  }
}
