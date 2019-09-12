package com.personal.spark.movielens.transformations

import org.apache.spark.rdd.RDD
import com.personal.spark.movielens.util.Ratings
import org.apache.spark.sql.Row
import com.personal.spark.movielens.util.Movies
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transformations {
  
	def transformRatingsRDD(rdd:RDD[Row],delimiter:String):RDD[Ratings]={
		rdd.map{row => 
  																	val colsData = row.getAs[String](0).replace("::",":").split(":")
  																	Ratings(colsData(0).toInt,colsData(1).toInt,colsData(2).toInt,colsData(3).toLong)}
	}
	
	def transformMovieRDD(rdd:RDD[Row],delimiter:String):RDD[Movies]={
		rdd.map{row => 
  																	val colsData = row.getAs[String](0).replace("::",":").split(":")
  																	Movies(colsData(0).toInt,colsData(1),colsData(2))}
	}
	
	def getUniqueUsers(df:DataFrame):DataFrame={
		df.groupBy(col("userId")).agg(min(col("movieId"))).select(col("userId"))
	}
	def getMostRatedMovie(df:DataFrame):DataFrame={
		df.groupBy(col("movieId")).agg(count(col("userId")).as("cnt"),avg(col("rating")).as("average_rating")).orderBy(col("cnt").desc)
	}
}