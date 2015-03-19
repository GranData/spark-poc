package poc

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object WholeTextFile extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.wholeTextFiles("/home/esteban/input*.txt", 4)
  println(lines.partitions.size)
  lines.map( t => println(Thread.currentThread + " " + t._1 + " => " + t._2)).count()

}