package org.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kristina Trifunovic
 */
object App {
  
  def main(args : Array[String]): Unit = {
    println( "Starting application..." )
    // Set up Spark configuration
    val conf = new SparkConf().setAppName("spark_training").setMaster("local[*]")
    val sc = new SparkContext(conf)

  }
}
