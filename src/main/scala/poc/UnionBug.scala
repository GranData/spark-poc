package poc

import org.apache.spark._
/**
 * The intention of this Scala App is to reproduce 2 bugs found in org.apache.spark.SparkContext.union
 * of Apache Spark 1.3 
 * 
 * These bugs are already fixed in version 1.4 so you have to upgrade to this version or implement 
 * the following workaround to bypass these bugs.
 * 
 * Workaround: whenever you have to union multiple RDDs you have to make sure that either all or none
 * of them have a partitioner defined. Otherwise, if some of them have a partitioner defined and others don't,
 * then you will experience any of the following bugs. 
 * 
 * @author Esteban Donato
 */
object UnionBug extends App {
  
  val sc = new SparkContext("local","union")
  val list1 = List(("1", "one"), ("2", "two"), ("3", "three"), ("4", "four"))
  val list2 = List(("1", "uno"), ("2", "dos"), ("3", "tres"), ("4", "cuatro"))
  
  /**
   * First scenario: We want to union 2 RDDs, one of them with a partitioner (rdd1) and the other one 
   * without it (rdd2).  Additionally they have a different number of partitions (4 partitions for 
   * rdd1 and 2 partitions for rdd2).
   * In this scenario you will get an ArrayIndexOutOfFoundsException
   * 
   */
  var rdd1 = sc.parallelize(list1).partitionBy(new HashPartitioner(4))
  var rdd2 = sc.parallelize(list2, 2)
  
  try sc.union(List(rdd1, rdd2)).collect
  catch { case e: ArrayIndexOutOfBoundsException => e.printStackTrace() }
  
  /**
   * Second scenario: We want to union 2 RDDs, one of them with a partitioner (rdd1) and the other one 
   * without it (rdd2). Additionally both RDDs have the same number of partitions.
   * In this scenario you will get a valid RDD with a partitioner defined. However since Spark assumes
   * that all the RDDs have the same partitioner you will get a RDD that may have elements with
   * the same key in different partitions. For instance, check that the two elements with key "1"
   * were assigned to partition 0 and 1 while the RDD expresses that it was hash-partitioned. 
   * This could be an issue if you plan to apply transformations to this RDD that take advantage of 
   * the partitioning strategy, like reduceByKey, join, lookup, etc
   */
  rdd1 = sc.parallelize(List(("1", "one"), ("2", "two"), ("3", "three"), ("4", "four"))).partitionBy(new HashPartitioner(4))
  rdd2 = sc.parallelize(List(("1", "uno"), ("2", "dos"), ("3", "tres"), ("4", "cuatro")), 4)
  
  val rddUnion = sc.union(List(rdd1, rdd2))
  rddUnion
    .mapPartitionsWithIndex { case (idx, partition) => partition.map(x => s"partition: ${idx}: ${x}") }
    .collect
    .sorted
    .foreach(println)
  println(s"partitioner ${rddUnion.partitioner}")
}