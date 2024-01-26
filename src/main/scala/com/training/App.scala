package com.training

import com.training.api.{CartAPI, DataManipulation, ProductAPI, UserAPI}
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
    val dataManipulation = new DataManipulation
//    val productAPI = new ProductAPI
//
//    val productsRDD = productAPI.getProducts
//    val reduceByKey = dataManipulation.reduceByKey(productsRDD)
//    val aggregateByKey = dataManipulation.aggregateByKey(productsRDD)
//
    val cartAPI = new CartAPI
    val userAPI = new UserAPI
    val carts = cartAPI.getCarts
    val users = userAPI.getUsers
    val joinUsersAndCarts = dataManipulation.join(carts, users)

    println("Ending application...")

  }
}
