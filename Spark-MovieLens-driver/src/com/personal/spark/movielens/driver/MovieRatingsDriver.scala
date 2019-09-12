package com.personal.spark.movielens.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import com.personal.spark.movielens.util.GenericUtils
import com.personal.spark.movielens.transformations.Transformations
import com.personal.spark.movielens.transformations.Transformations

object MovieRatingsDriver {
  def main(args:Array[String]){
   	
  	val basePath = args(0)+"/"
  	val spark = GenericUtils.spark
  	val sparkContext=GenericUtils.sparkContext
  	
  	/*
  	 * Expected output ratings file sample 
  	 */
  	//val sparkContext = new SparkContext(sparkConf)
  	val sqlContext = new SQLContext(sparkContext)
  	val ratingsRDD = Transformations.transformRatingsRDD(spark.read.text(basePath+"ratings.dat").rdd, "") 
  																
  	
  	val ratingsDf = spark.createDataFrame(ratingsRDD).repartition(10).cache()
  	ratingsDf.show();
  	println(ratingsDf.schema)
  	
  	/*
  	 * Expected output movie file sample 
  	 */
  	val moviesRDD = Transformations.transformMovieRDD(spark.read.option("delimiter","::").text(basePath+"movies.dat").rdd,"")
  																
  	
  	val moviesDf = spark.createDataFrame(moviesRDD).repartition(10).cache()
  	moviesDf.show();
  	println(moviesDf.schema)
  	
  	/*
  	 * Unique users
  	 */
  	val distinctUidDF = Transformations.getUniqueUsers(ratingsDf)
  	distinctUidDF.show()
  	
  	/*
  	 * Most rated movie
  	 */
    val mostRatedMoviesDF = Transformations.getMostRatedMovie(ratingsDf)
    val topRecMostRatedDF = mostRatedMoviesDF.limit(1)
  	topRecMostRatedDF.show()
  	
  
  }
}