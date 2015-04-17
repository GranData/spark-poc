package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Lookup extends App{
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.parallelize(List((1,"one"), (2, "two")))
  
  println(lines.lookup(1).isEmpty)
  println(lines.lookup(3).isEmpty)
  lines.lookup(1).foreach(println(_))
}