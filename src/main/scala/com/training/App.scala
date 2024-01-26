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
    val productAPI = new ProductAPI

    val products = productAPI.getProducts
    val reduceByKey = dataManipulation.reduceByKey(products)
    val aggregateByKey = dataManipulation.aggregateByKey(products)

    val cartAPI = new CartAPI
    val userAPI = new UserAPI
    val carts = cartAPI.getCarts
    val users = userAPI.getUsers
    val joinUsersAndCarts = dataManipulation.join(carts, users)
    val cogroup = dataManipulation.cogroup(carts, users)
    val cartesian = dataManipulation.cartesian(carts, users)
    val pipe = dataManipulation.pipe(products)
    println(s"officially has appeared ${pipe.count()} times in products description")

    println("Ending application...")

  }
}
