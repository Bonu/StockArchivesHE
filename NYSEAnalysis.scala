
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object NYSEAnalysis {

//  case class NYSEDaily(exchange: String, stockSymbol: String, date: String,
//                       openPrice: Double, highPrice: Double,
//                       lowPrice: Double, closePrice: Double,
//                       volume: Int, ajClosePrice: Double)
//
//
//  case class NYSEDividents(exchange: String, stockSymbol: String, date: String, dividends: String)


  def main(args: Array[String]): Unit = {

    val nyseDailySchema = StructType(StructField("exchange", StringType, true) ::   StructField("stockSymbol", StringType, true) ::   StructField("date", StringType, true) ::   StructField("openPrice", DoubleType, true) ::   StructField("highPrice", DoubleType, true) ::   StructField("lowPrice", DoubleType, true) ::   StructField("closePrice", DoubleType, true) ::   StructField("volume", IntegerType, true) ::   StructField("adjClosePrice", DoubleType, true) :: Nil
    )

    val conf = new SparkConf().setMaster("local").setAppName("NYSE-Stock-Analysis")

    val spark = SparkSession
      .builder()
      .appName("NYSE-Stock-Analysis").config(conf)
      .getOrCreate()

    val nyseDailyData = spark.read.format("com.databricks.spark.csv").schema(nyseDailySchema).option("header", "false").option("delimiter", " \t").load("/user/JigSGB221/sparksql/input/NYSE_daily-1.tsv")

    nyseDailyData.createOrReplaceTempView("NYSE_DAILY_VIEW")

    val nyseDividentsSchema = StructType(StructField("exchange", StringType, true) ::   StructField("stock_symbol", StringType, true) ::   StructField("date", StringType, true) ::   StructField("dividends", DoubleType, true) :: Nil  )
    val input_div = spark.read.format("com.databricks.spark.csv").schema(nyseDividentsSchema).option("header", "false").  option("delimiter", "\t").load("/user/JigSGB221/sparksql/input/NYSE_dividends-1.tsv"  )

    input_div.createOrReplaceTempView("NYSE_DIVIDENTS_VIEW")

    //  Listing of records from daily data which have the stock close price more than or equal to 200 and stock volume more than or equal to 10 million. 
    val stocks = spark.sql("select * from NYSE_DAILY_VIEW where close_price >= 200 and volume >= 1000000")

    //  Listing of companies (symbols) and the number of times (count) dividends are given based on the dividends data for all companies that have given dividends more than 50 times.
    val dividents = spark.sql("SELECT stock_symbol as COMPANY , COUNT(dividends) from NYSE_DIVIDENTS_VIEW group by stock_symbol having COUNT(dividends) > 50")

    //	Listing of company symbol, close price, dividends and the date for all records matching the symbol and date in daily data and dividends data with daily close price more than or equal to 100.
    val stocksDividents = spark.sql("select a.stock_symbol,a.close_price,b.dividends,a.date from NYSE_DAILY_VIEW a, NYSE_DIVIDENTS_VIEW b where a.stock_symbol = b.stock_symbol and a.date=b.date and a.close_price >= 100 ")

    stocks.write.format("com.databricks.spark.csv").save("/user/JigSGB221/sparksql/csvout/stocks/")
    dividents.write.format("com.databricks.spark.csv").save("/user/JigSGB221/sparksql/csvout/dividents/")
    stocksDividents.write.format("com.databricks.spark.csv").save("/user/JigSGB221/sparksql/csvout/stocksDividents/")

  }

}
