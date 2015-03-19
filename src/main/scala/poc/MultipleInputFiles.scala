package poc

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object MultipleInputFiles extends App {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("/home/esteban/input*.txt")//.repartition(2)
  //val lines = sc.textFile("/d*/sms/[1-2]/*.txt").union(sc.textFile("/d*/voice/*/*.txt"))
  
  println(lines.map(x => {
    println(Thread.currentThread + " " + x)
    x
  }).count())
}