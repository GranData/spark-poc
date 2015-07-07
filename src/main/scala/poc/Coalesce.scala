package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

object Coalesce extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  val rdd = sc.parallelize(Array(1,2,3,4), 2)
  
  val rdd1 = rdd.mapPartitionsWithIndex {
    case (idx, v) => println("partition " + idx + " thread " + Thread.currentThread().getName); v
  }.coalesce(1)
  
  rdd1.count()
  sc.stop()  

}