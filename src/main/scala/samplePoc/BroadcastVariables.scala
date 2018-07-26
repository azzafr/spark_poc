package samplePoc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object BroadcastVariables {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("local[*]")
        .config("spark.driver.bindAddress","127.0.0.1")
        .appName("BroadcastVariables")
        .getOrCreate()

    val broadcastVars: Broadcast[List[Int]] = spark.sparkContext.broadcast(List(1,2,3,4,5,6,7,8,9))
    val broadcastVarsValue: Seq[Int] = broadcastVars.value


  }
}
