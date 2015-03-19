package poc

import org.apache.spark._
import org.apache.spark.SparkContext._

object Pipe extends App {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("/home/esteban/input.txt")
  
  lines.collect.foreach(x => println(x))
  
  lines.pipe("/home/esteban/pipe.sh").collect.foreach(x => println(x))
}