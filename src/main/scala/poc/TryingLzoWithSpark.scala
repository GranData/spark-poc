package poc

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by marcos on 21/09/15.
 */
object TryingLzoWithSpark extends App{


  /**
   * Note:  Use this VM options for run this example:   *
   * -Djava.library.path=/path/to/hadoop-lzo/target/native/Linux-amd64-64
   *
   * Or for this self-contained project use:   *
   * -Djava.library.path=src/main/resources/hadoop-lzo/native/Linux-amd64-64
   * */

  val sparkConf = new SparkConf().setAppName(s"A example of Spark with Lzo").setMaster("local")

  val sc = new SparkContext(sparkConf)
  sc.hadoopConfiguration.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")

  val lzoFile = "hdfs://localhost:9000/input/customers.txt.lzo"
  val linesRdd = sc.newAPIHadoopFile(lzoFile, classOf[com.hadoop.mapreduce.LzoTextInputFormat],classOf[org.apache.hadoop.io.LongWritable],classOf[org.apache.hadoop.io.Text]).map(_._2.toString())


  val columnsCountPerLine = linesRdd.map(_.split(s"\\|", -1).toList).map(_.size)
  val haveAllLines71Colums = columnsCountPerLine.take(1000).map(_.equals(71)).reduce(_ && _)


  println("Print Some lines:"); linesRdd.take(10).foreach(println)
  println("Partitions Count:"+ linesRdd.partitions.size)
  println("Do all columns have 71 columns?:" + haveAllLines71Colums)
}

