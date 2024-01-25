package com.training

import com.training.api.{DataManipulation, ProductAPI}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @author Kristina Trifunovic
 */
object App {
  // Set up Spark configuration
  val spark: SparkSession = SparkSession.builder.appName("spark_training").master("local[*]").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  
  def main(args : Array[String]): Unit = {
    println( "Starting application..." )
    val productAPI = new ProductAPI
    val dataManipulation = new DataManipulation

    val productsRDD = productAPI.getProducts
    val reduceByKey = dataManipulation.reduceByKey(productsRDD)
    val aggregateByKey = dataManipulation.aggregateByKey(productsRDD)

    println("Ending application...")

  }
}
