package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.IOException
import java.io.ObjectInputStream
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.HashPartitioner
object Cache extends App {
  System.clearProperty("spark.driver.port")
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")//.set("spark.driver.port","")
  val sc = new SparkContext(conf)
  Cache1.run(sc)
}
object Cache1 {
  
  def run(sc: SparkContext) = {
   
  val rdd = sc.parallelize(Array(1,2,3,4), 2)
  
  val rdd1 = rdd.map { x =>
    println("map " + x)
    x*10
  }.map { x => 
    println("map2 " + x)
    (x,x)
  }.partitionBy(new HashPartitioner(3))//.reduceByKey(_+_)
  //}
  
  val rdd2 = rdd1.map { x =>
    println("map rdd1 to rdd2")
    x
  }
  //rdd1.cache()
  
  rdd1.count()
  rdd2.count()
  sc.stop()  
  }
  
}