package com.training.api

import com.training.App.sc
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import upickle.default._

import java.net.URL

class ProductAPI {

  def getProducts: RDD[Product] = {
    val url = new URL("https://dummyjson.com/products")
    val lines = IOUtils.toString(url, "UTF-8")
    val productContainer = upickle.default.read[ProductContainer](lines)
    sc.parallelize(productContainer.products)
  }

  def skipNthNumOfProductsLimit10(numOfProductsToSkip: Int): RDD[Product] = {
    val url = new URL(s"https://dummyjson.com/products?skip=$numOfProductsToSkip&limit=10")
    val lines = IOUtils.toString(url, "UTF-8")
    val productContainer = upickle.default.read[ProductContainer](lines)
    sc.parallelize(productContainer.products)
  }

}

case class ProductContainer(products: List[Product])
case class Product(id: Int, title: String, description: String, price: Int, discountPercentage: Int,
                   rating: Int, stock: Int, brand: String, category: String, thumbnail: String, images: List[String])

object Product {
  implicit val productRW: ReadWriter[Product] = macroRW
}

object ProductContainer {
  implicit val productContainerRW: ReadWriter[ProductContainer] = macroRW
}