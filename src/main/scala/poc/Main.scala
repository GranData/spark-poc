package poc

import java.io.File
import java.nio.file.{Files, FileSystems}
import java.text.{DateFormatSymbols, SimpleDateFormat}
import java.util.{Locale, Date}

import org.apache.spark._
import org.joda.time.DateTimeConstants
import scala.language.postfixOps

import java.sql.Timestamp

case class VoiceRecord(contractNumber: String, mdn: String, source: String, target: String, durationSeconds: Int, incomeWithoutTaxes: Float, operator: String, redirect:String, timestamp: Timestamp)

object Main {
  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
    else Stream.empty)

  def time[A](a: => A) = {
    val now = System.nanoTime
    val result = a
    val micros = (System.nanoTime - now) / 1000
    println("%d microseconds".format(micros))
    result
  }

  def main(args: Array[String]) = {
    // WARNING: I am doing nothing about the timezone or the encoding.\
    // WARNING: Quick and easy but not efficient. Should not be used in production.
    val linksFileNames = getFileTree(new File("/home/gustavo/work/gd/new-pipeline/")).filter { file =>
      !(".*/data[1-4]/iusacell/.*/.*/voice/.*.gz".r findFirstIn file.getAbsolutePath).isEmpty
    }.map(_.getAbsolutePath)

    println("Files :")
    linksFileNames.foreach(println)

    val conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application")
    val sc = new SparkContext(conf)


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // Just drop header line of each file and the merge them. It is not pretty. We should find a better way. Also. we must validate header using expected schema.
    val dataFile = linksFileNames.map { f=>sc.textFile(f).zipWithIndex.filter(_._2 >= 10L).map(_._1)  }.reduce(_++_)

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss a")
    val symbols = new DateFormatSymbols(Locale.getDefault())
    symbols.setAmPmStrings(Array("a.m.", "p.m."))
    dateFormat.setDateFormatSymbols(symbols)

    val parsingErrorsAccum = sc.accumulator(0, "Parsing errors")

    val voiceData = dataFile
      .map(_.split("\t"))
      .flatMap { row =>
        try {
          /*
            Hay que chequear esto, pero en principio, cuando nos llega una llamada entre dos clientes, van a aparecer dos
            registros uno para la llamada entrante y otro para la saliente.


            Los casos son:
              Operador: IUSA:
                IUSA (a) -> IUSA (b): 2 registros (saliente y entrante).  => Descarto una la entrante
                  source: a
                  target: b

                IUSA (a) -> Otra (b): 1 registro (saliente)
                  source: a
                  target: b

              Operador: Otra:
                Otra (a) <- IUSA (a): 1 registro (entrante)
                  source: b
                  target: a

           */
          val operator = row(8).split(" ").last.trim.toLowerCase
          val direction = row(4).trim.toLowerCase

          if ((operator == "iusacell") && (direction == "entrante")) {
              None
          } else {
            val (source, target) = if ((operator == "iusacell") && (direction == "saliente")) {
              (row(2).trim, row(3).trim)
            } else {
              (row(3).trim, row(2).trim)
            }

            val timestamp = dateFormat.parse(row(9).trim)

            Some(VoiceRecord(
              contractNumber = row(0).trim,
              mdn = row(1).trim,
              source = row(2).trim,
              target = row(3).trim,
              durationSeconds = row(5).trim.toInt,
              incomeWithoutTaxes = row(6).toFloat,
              redirect = row(7),
              operator = row(8).split(" ").last.trim.toLowerCase,
              timestamp = new Timestamp(timestamp.asInstanceOf[Date].getTime)))
          }
        } catch {
          case _: Throwable =>
            parsingErrorsAccum += 1
            println(s"Time error: ${row.reduce(_ ++ "---" ++  _)}")
            None
        }
    }

    val voiceDF = voiceData.toDF

    case class VoiceLink(totalCalls: Int, timeTotal: Int, callsWeekend: Int, timeWeeked: Int, callsWeekDaylight: Int, timeWeekDaylight: Int, callsWeekNight: Int, timeWeekNight: Int) {
      def +(that: VoiceLink) =
        VoiceLink(
          this.totalCalls + that.totalCalls,
          this.timeTotal + that.timeTotal,
          this.callsWeekend + that.callsWeekend,
          this.timeWeeked + that.timeWeeked,
          this.callsWeekDaylight + that.callsWeekDaylight,
          this.timeWeekDaylight + that.timeWeekDaylight,
          this.callsWeekNight + that.callsWeekNight,
          this.timeWeekNight + that.timeWeekNight
        )

    }
    time {
      voiceData.map { record =>
        import com.github.nscala_time.time.Imports._

        val timestamp = new DateTime(record.timestamp)

        val (callsWeekend: Int, timeWeekend: Int, callsWeekDaylight: Int, timeWeekDaylight: Int, callsWeekNight: Int, timeWeekNight: Int) = timestamp.getDayOfWeek match {
          case DateTimeConstants.SUNDAY | DateTimeConstants.SATURDAY =>
            (1, record.durationSeconds, 0, 0, 0, 0)
          case _ =>
            if (9 until 19 contains timestamp.getHourOfDay)
              (0, 0, 1, record.durationSeconds, 0, 0)
            else
              (0, 0, 0, 0, 1, record.durationSeconds)
        }

        ((record.source, record.target), VoiceLink(
          totalCalls = 1,
          timeTotal = record.durationSeconds,
          callsWeekend = callsWeekend,
          timeWeeked = timeWeekend,
          callsWeekDaylight = callsWeekDaylight ,
          timeWeekDaylight = timeWeekDaylight ,
          callsWeekNight = callsWeekNight ,
          timeWeekNight = timeWeekNight
        ))
      }.reduceByKey(_ + _).saveAsTextFile("/tmp/output.txt")
    }

    voiceDF.registerTempTable("voice")

    val query = "SELECT operator, count(*) c FROM voice GROUP BY operator";

    println(s"Result: ")

//    OriginLineSurrKey|TargetLineSurrKey|CallsWeekDaylight|CallsWeekNight|CallsWeekend|CallsTotal|TimeWeekDaylight|TimeWeekNight|TimeWeekend|TimeTotal|DscntFFVoice|ContactDaysVoice
    time {
      sqlContext.sql(query).collect.foreach(println)
    }

//    println(s"Count: ${dataFile.count()}")

    println(s"Parsing errors: ${parsingErrorsAccum.value}")


//    val pData = sc.parallelize(Vector(1,2,3,4,5,6,7,8,9,19), 10)
//    dataFile.persist(StorageLevel.MEMORY_AND_DISK)
//
//    //TODO check that map is actually a lazy operation. it is just executed when an operation is called for each operation
//    val lines = sc.textFile("data.txt")
//    val lineLengths = lines.map(s => s.length)
//    //TODO check RDD persistance
//    //pData.persist()
//    val totalLength = lineLengths.reduce((a, b) => a + b)
//
//
//    def sumLines(l1: String, l2:String): String = {
//      val values1 = l1.split('|')
//      val values2 = l2.split('|')
//
//      var row = ""
//      for (i <- 0 to 8) {
//        row = row + (values1(i).toInt+values2(i).toInt).toString+"|"
//      }
//      row = row + values1(9) + values2(9)
//
//      return row
//    }
//
//
//    def splitInKeyValue(voiceRow: String): (String, String)= {
//
//      val values = voiceRow.split('|')
//      var key = values(0)+"-"+values(1)
//
//      var value=""
//      for (i <- 2 to 10) {
//        value = value + values(i)+"|"
//      }
//      value = value + values(11)
//      return (key, value)
//    }
//
//    println(dataFile.count());
//    println("------------------------------------------------------------------------------------------------------------------------------------------")
//    println(dataFile.count());
//    dataFile.map(line => (splitInKeyValue(line))).reduceByKey((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
//    //dataFile.map(line => (splitInKeyValue(line))).reduce((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
//    //dataFile.map(line => (splitInKeyValue(line))).reduce((a, b) => sumLines(a,b)).saveAsTextFile("spark-out-gz")
//    //dataFile.map(line => (splitInKeyValue(line))).r
//    //dataFile.r
//
//    //dataFile.re
  }
}