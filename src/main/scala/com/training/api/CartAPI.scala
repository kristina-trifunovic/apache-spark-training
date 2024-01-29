package com.training.api

import com.training.TransformationMethods.sc
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import upickle.default._

import java.net.URL

class CartAPI {

  def getCarts: RDD[Cart] = {
    val url = new URL("https://dummyjson.com/carts?limit=3")
    val lines = IOUtils.toString(url, "UTF-8")
    val cartContainer = upickle.default.read[CartContainer](lines)
    sc.parallelize(cartContainer.carts)
  }

}

case class CartContainer(carts: List[Cart])
case class Cart(id: Int, products: List[ProductFromCart], total: Int, discountedTotal: Int, userId: Int, totalProducts: Int, totalQuantity: Int)
case class ProductFromCart(id: Int, title: String, price: Int, quantity: Int, total: Int, discountPercentage: Int,
                           discountedPrice: Int, thumbnail: String)
case class ProductFromCartContainer(products: List[ProductFromCart])

object Cart {
  implicit val cartRW: ReadWriter[Cart] = macroRW
}

object CartContainer {
  implicit val cartContainerRW: ReadWriter[CartContainer] = macroRW
}
object ProductFromCart {
  implicit val productFromCartRW: ReadWriter[ProductFromCart] = macroRW
}

object ProductFromCartContainer {
  implicit val productFromCartContainerRW: ReadWriter[ProductFromCartContainer] = macroRW
}
