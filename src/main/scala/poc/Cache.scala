package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Cache extends App {
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val rdd = sc.parallelize(Array(1,2,3,4))
  
  val rdd1 = rdd.map { x =>
    println("map " + x)
    x*10
  }.map { x => 
    println("map " + x)
    (x,x)
  //}.reduceByKey(_+_)
  }.cache
  
  println("count " + rdd1.count())
  println("max " + rdd1.max())
  sc.stop()
}