package poc

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ParallelizedCollection extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val data = Array(1, 2, 3, 4, 15, 16, 17, 18)
  val distData = sc.parallelize(data, 4)
  
  val lineLengths = distData.map(s => {
    
    println(Thread.currentThread + " " + s)
    s * 10
  })
  
  println(lineLengths.reduce((a, b) => a+b))
  printf(Thread.currentThread + " finishing")
}