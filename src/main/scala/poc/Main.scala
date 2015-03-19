package poc

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]) = {
    val linksFilename = "/home/esteban/iusacell/fullinput.txt"
    
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val dataFile = sc.textFile(linksFilename, 2).cache()
    val pData = sc.parallelize(Vector(1,2,3,4,5,6,7,8,9,19), 10)
    dataFile.persist(StorageLevel.MEMORY_AND_DISK)
    
    //TODO check that map is actually a lazy operation. it is just executed when an operation is called for each operation
    val lines = sc.textFile("data.txt")
    val lineLengths = lines.map(s => s.length)
    //TODO check RDD persistance
    //pData.persist()
    val totalLength = lineLengths.reduce((a, b) => a + b)
    

    def sumLines(l1: String, l2:String): String = {
      val values1 = l1.split('|')
      val values2 = l2.split('|')
  
      var row = ""
      for (i <- 0 to 8) {
        row = row + (values1(i).toInt+values2(i).toInt).toString+"|"
      }
      row = row + values1(9) + values2(9)
  
      return row
    }


    def splitInKeyValue(voiceRow: String): (String, String)= {
    
      val values = voiceRow.split('|')
      var key = values(0)+"-"+values(1)
      
      var value=""
      for (i <- 2 to 10) {
        value = value + values(i)+"|"
      }
      value = value + values(11)
      return (key, value)
    }
    
    println(dataFile.count());
    println("------------------------------------------------------------------------------------------------------------------------------------------")
    println(dataFile.count());
    dataFile.map(line => (splitInKeyValue(line))).reduceByKey((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
    //dataFile.map(line => (splitInKeyValue(line))).reduce((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
    //dataFile.map(line => (splitInKeyValue(line))).reduce((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
    //dataFile.map(line => (splitInKeyValue(line))).r
    //dataFile.r

    //dataFile.re
  }
}