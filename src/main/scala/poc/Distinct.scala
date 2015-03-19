package poc

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object Distinct extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.parallelize(Array(1, 2, 1, 2))
  println("array partition size " + lines.partitions.size)
  
  val distinct = lines.distinct();
  
  println("distinct " + distinct.partitions.size)
  distinct.collect.foreach(x => println(x))
  distinct.map( t => println(Thread.currentThread + " " + t)).count()
  
  val fullLines = sc.textFile("/home/esteban/iusacell/fullinput.txt")
  println("full lines partitions " + fullLines.partitions.size)
  val t = System.currentTimeMillis()
  val fullCount = fullLines.distinct(2)
  println("full count partitions " + fullCount.partitions.size)
  val count = fullCount.count()
  println("distinct time " + (System.currentTimeMillis() - t) + " count " + count)
  

}