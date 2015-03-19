package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object ParallelReduces extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("/home/esteban/input*.txt").repartition(4)//.map(x => x + " from map")
  println(lines.partitions.size)
  
  
  lines.reduce((x, y) => {
    println(Thread.currentThread + " => " + x + " --- " + y)
    if(x.compareTo(y) < 0)
      x
    else
      y
  });
  println("------------------------------------")
  val redByKey = sc.parallelize(Array("1uno", "1dos","1uno'", "3tres", "3cuatro", "1cuatro'", "2cinco", "2seis", "1seis'"), 3)
  //val redByKey = sc.textFile("/home/esteban/iusacell/fullinput.txt")
    .map(x => (x.charAt(0), x)).reduceByKey((x, y) => {
      println(Thread.currentThread + " => " + x + " --- " + y)
      if(x.compareTo(y) < 0)
        x
      else
        y
    })
  println("partitions " + redByKey.partitions.size)
  println(redByKey.count())
  
  println("------------------------------------")
  val groupByKey = sc.parallelize(Array("1uno", "1dos","2tres", "2cuatro")).map(x => (x.charAt(0), x)).groupByKey().collect()
    .foreach(tuple => {
      println(tuple._1)
      tuple._2.foreach(value => printf(" %s", value))
      println()
    })
    
  println("------------------------------------")
  val aggByKey = sc.parallelize(Array("1uno", "1dos","1tres", "1cuatro"), 2).map(x => (x.charAt(0), x)).aggregateByKey(0)(
      (x,y) => {
        x+y.length()
      }, 
      (x,y) => {
        y+x
      }).foreach(t => println(t._1 + " " + t._2))
    
}