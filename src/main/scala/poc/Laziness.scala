package poc

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Laziness extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("/home/esteban/input.txt")
  val lineLengths = lines.map(s => {
    
    println(s)
    s.length 
  })
  
  println("caching")
  lineLengths.cache() 
  println(lineLengths.reduce((a, b) => Math.max(a, b)))
  println("counting")
  println(lineLengths.count())
  println(lineLengths.count())
  printf("finishing")
}