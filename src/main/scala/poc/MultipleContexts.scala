package poc

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MultipleContexts extends App {
  
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test")
    .set("spark.hadoop.validateOutputSpecs", "false")
    //.set("spark.driver.allowMultipleContexts" , "true")
    )
  val r = sc.parallelize(List(12,3))
  r.count()
  sc.stop()
  val sc1 = new SparkContext(new SparkConf().setMaster("local").setAppName("test")
    .set("spark.hadoop.validateOutputSpecs", "false")
    //.set("spark.driver.allowMultipleContexts" , "true")
    )
  val r1  = sc1.parallelize(List(12,3))
  r1.count()
}