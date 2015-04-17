/**
 *
 */
package poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner

/**
 * @author esteban
 *
 */
object ZipWithIndex extends App {
  
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val lines = sc.parallelize(Array("one", "two", "three", "four", "five"), 2)
  
  println(lines.partitioner)
  val zip = lines.zipWithIndex.partitionBy(new HashPartitioner(2))
  zip.collect.foreach(println(_))
  println(zip.partitioner)
  println("--------------")
  val zipu = lines.zipWithUniqueId
  zipu.collect.foreach(println(_))
  println(zipu.partitioner)
}