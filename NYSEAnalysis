
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql._

import math._

object NYSEAnalysis {

  case class NYSEDaily(exchange: String, stockSymbol: String, date: String,
                       openPrice: Double, highPrice: Double,
                       lowPrice: Double, closePrice: Double,
                       volume: Int, ajClosePrice: Double)


  case class NYSEDividents(exchange: String, stockSymbol: String, date: String, dividends: String)


  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val textRDD = sc.textFile("src\\main\\resources\\emp_data.csv")
    //println(textRDD.foreach(println)
    val empRdd = textRDD.map {
      line =>
        val col = line.split("\t")
        NYSEDaily(col(0), col(1), col(2), col(3).toDouble, col(4).toDouble, col(5).toDouble, col(6).toDouble, col(7).toInt, col(8).toDouble)
    }
    val empDF = empRdd.toDF()
    empDF.show()
    /* Spark 2.0 or up
      val empDF= sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("src\\main\\resources\\emp_data.csv")
     */
  }
//  def test(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setMaster("local").setAppName("Movie-Data-Analysis")
//
//    val spark = SparkSession
//      .builder()
//      .appName("SparkUserData").config(conf)
//      .getOrCreate()
//
//    val nyseDailyData = spark.read.format("com.databricks.spark.csv").schema(NYSEDaily).option("header", "false").option("delimiter", " \t").load("/mydata/NYSE_daily-1.tsv")
//
//  }
}
