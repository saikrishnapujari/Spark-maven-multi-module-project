package com.personal.spark.movielens.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object GenericUtils {
	/*
  	 * For testing in windows os - with eclipse
  	 * Steps::
  	 * 
  	 * Create the following directory structure: "C:\hadoop_home\bin" (or replace "C:\hadoop_home" with whatever you like)
		 * Download the following file: http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
     * Put the file from step 2 into the "bin" directory from step 1.
	   * Set the "hadoop.home.dir" system property to "C:\hadoop_home" (or whatever directory you created in step 1, without the "\bin" at the end). Note: You should be declaring this property in the beginning of your Spark code
  	 */
  	sys.props.+=(("hadoop.home.dir","C:\\hadoop_home"))
    val sparkConf = new SparkConf().setMaster("local").setAppName("Sample")
  	val sparkContext = new SparkContext(sparkConf)
  	val spark = SparkSession.builder().appName("Sample").getOrCreate()
  	
}