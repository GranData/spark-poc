package poc

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ChainedTransformations extends App {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.parallelize(Array(1, 2, 3, 4), 4)
  
  lines.map(x => {
   println("from map " + x)
   x*10 
  }).filter( x => {
   println("filter " + x)
   x < 25
  }).collect().foreach(x => println(x))
  
  lines.map(x => {
   println("from map " + x)
   (x%2, x*10) 
  }).filter( x => {
   println("filter " + x)
   x._2 < 55
  }).reduceByKey((x, y) => {
    println("reduce by key " + x)
    x+y
  }).collect().foreach(x => println(x))
  
  println("--------------------------------------")
  
  lines.mapPartitions(x => {
    println("map partitions " + x)
    x.map(y => {
      println("in map o iteration " + y)
      y*10
    })
  }).filter( x => {
   println("filter " + x)
   x < 55
  }).count()
  
  println("--------------------------------------")
  
  lines.flatMap(x => {
    println("flatMap " + x)
    Array(x, 10*x, 100*x)
  }).filter( x => {
   println("filter " + x)
   x < 300
  }).collect().foreach { x => println(x) }
}