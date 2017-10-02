package com.SparkRecipies.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Integration {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("Integration")
      .getOrCreate()
      
      var a = 1
      var b = 5
      var dx = .1
      
      val ts = session.sparkContext.parallelize(1 to ((b-a)./(dx)).toInt)
      ts.foreach { println }
      var integration_result = ts.map{ x => var y = x.*(dx); (1,dx*((func(y.toDouble)+func(y.toDouble+dx))./(2))) }
      integration_result.foreach(println)
      var result = integration_result.reduceByKey((x1,x2) => (x1+x2))
      result.foreach(println)
      
      
  }
  def func(x:Double):Double = {
    return -0.25.*(x.*(x)) + x + 4
  }
}
