package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Join extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rdd1 = sc.parallelize(List((1,"uno"), (2,"dos"), (2,"two")))
  val rdd2 = sc.parallelize(List((3,"tres"), (2,"dos"), (2,"dos'")))
  
  val rr = rdd1.mapPartitions(part => {
    part.map{case (id, value) => throw new RuntimeException}
  })
  
  rr.saveAsTextFile("/tmp/rr")
  
  rdd1.fullOuterJoin(rdd2).collect().foreach(println(_))
}