package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.IOException
import java.io.ObjectInputStream
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
object Cache extends App {
  //System.clearProperty("spark.driver.port")
  val conf = new SparkConf()
    .setAppName("Simple Application")
    .setMaster("spark://esteban:7077")
    .set("spark.eventLog.enabled", "true")
  val sc = new SparkContext(conf)
  Cache1.run(sc)
}
object Cache1 {
  
  def run(sc: SparkContext) = {
  try{ 
  val rdd = sc.textFile("/home/esteban/data/numbers.txt")
  
  val rdd1 = rdd.mapPartitions { x =>
    println("map rdd1, partition running on thread " + Thread.currentThread().getName)
    x
  }.map { x => 
    //println("map2 " + x)
    (x,x)
  }.setName("rdd1").repartition(4).cache()
  //rdd1.count()
  
  val rdd2 = rdd1.map { x =>
    //println("map rdd1 to rdd2")
    x
  }
  
 
  
  println("count!!!!!!!!!!!!!!!!!!! " + (rdd1 join rdd2).count())
  //println("count!!!!!!!!!!!!!!!!!!! " + (rdd1 join rdd3).count())
  }
  finally sc.stop()  
  }
  
}