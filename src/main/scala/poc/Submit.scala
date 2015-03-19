package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Submit extends App {
  val conf = new SparkConf().setAppName("First Submit").set("spark.cores.max", "3")
  val sc = new SparkContext(conf)
  
  val fullLines = sc.textFile("/home/esteban/iusacell/fullinput.txt")
  val fullCount = fullLines.distinct(2) 
  println("count " + fullCount.count())
}