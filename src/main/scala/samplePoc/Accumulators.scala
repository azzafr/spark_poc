package samplePoc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Accumulators {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Accumulators")
      .config("spark.driver.bindAddress","127.0.0.1")
      .master("local[*]")
      .getOrCreate()

    val acc: LongAccumulator = spark.sparkContext.longAccumulator("Sum")
    println("acc : "+acc)

    val list = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8,9))
    list.foreach(elem => acc.add(elem))

    println("acc : "+acc)
    println(acc.value) //45
    println(acc.count) //9
    println(acc.avg) //5.0
    println(acc.isZero) //false
    println(acc.sum) //45

  }
}
