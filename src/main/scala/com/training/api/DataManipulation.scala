package com.training.api

import org.apache.spark.rdd.RDD

class DataManipulation {

  def reduceByKey(products: RDD[Product]): RDD[(String, Int)] = {
    val mappedProducts = products.map(product => (product.category, product.price))
    mappedProducts.reduceByKey((price1, price2) => price1 + price2)
  }

  def aggregateByKey(products: RDD[Product]): RDD[(String, Int)] = {
    val mappedProducts = products.map(product => (product.category, product))
    def addOperator = (accumulator: Int, element: Product) => accumulator + element.price
    def combOperator = (accumulator1: Int, accumulator2: Int) => accumulator1 + accumulator2
    mappedProducts.aggregateByKey(0)(addOperator, combOperator)
  }

  def join(carts: RDD[Cart], users: RDD[User]): RDD[(Int, (Cart, User))] = {
    val mappedCarts = carts.map(cart => (cart.userId, cart))
    val mappedUsers = users.map(user => (user.id, user))
    mappedCarts.join(mappedUsers)
  }

}
