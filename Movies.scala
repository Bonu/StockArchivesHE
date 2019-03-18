package com.jsa.spark.rating

import org.apache.spark._
import org.apache.spark.sql._


object Movies {

  case class Movie(releaseYear:Int, length:Int, title:String, subject:String, actor:String, actress:String, director:String, popularity:Int, award:String)

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Movie-Data-Analysis")

    val spark = SparkSession
      .builder()
      .appName("SparkUserData").config(conf)
      .getOrCreate()

    val movieData = spark.sparkContext.textFile("src/main/resources/MovieDataSet.txt")

    val movieDS = movieData.map{record => val
    fields = record.split(";")

      var releaseYear = 0;
      if(!fields(0).isEmpty){
        releaseYear = fields(0).toInt
      }

      var length = 0;
      if(!fields(1).isEmpty){
        length = fields(1).toInt
      }

      val title = fields(2)
      val subject = fields(3)
      val actor = fields(4)
      val actress = fields(5)
      val director = fields(6)

      var popularity = 0;
      if(!fields(7).isEmpty){
        popularity = fields(7).toInt
      }

      val award = fields(8)

      Movie(releaseYear, length, title, subject, actor, actress, director, popularity, award)
    }

    // filter the rows having popularity null
    val movieDecade = movieDS.groupBy(movie => movie.releaseYear/10);

    val movieMax = movieDecade.mapValues(_.maxBy(_.popularity))

    movieMax.foreach(movie => println("Decade -> "+movie._1*10 +" Tile -> "+ movie._2.title+ " Popularity -> "+ movie._2.popularity))


  }

}
