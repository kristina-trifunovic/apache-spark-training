package com.training.api

import com.training.App.sc
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import upickle.default._

import java.net.URL

class UserAPI {

  def getUsers: RDD[User] = {
    val url = new URL("https://dummyjson.com/users?limit=3")
    val lines = IOUtils.toString(url, "UTF-8").replace("\"type\"", "\"hairType\"")
    val userContainer = upickle.default.read[UserContainer](lines)
    sc.parallelize(userContainer.users)
  }

}

case class UserContainer(users: List[User])
case class User(id: Int, firstName: String, lastName: String, maidenName: String, age: Int, gender: String, email: String,
                phone: String, username: String, password: String, birthDate: String, image: String, bloodGroup: String,
                height: Int, weight: Double, eyeColor: String, hair: Hair, domain: String, ip: String, address: Address,
                macAddress: String, university: String, bank: Bank, company: Company, ein: String, ssn: String,
                userAgent: String, crypto: Crypto)
case class Address(address: String, city: String, coordinates: Coordinates, postalCode: String, state: String)
case class Coordinates(lat: Double, lng: Double)
case class Bank(cardExpire: String, cardNumber: String, cardType: String, currency: String, iban: String)
case class Company(address: Address, department: String, name: String, title: String)
case class Crypto(coin: String, wallet: String, network: String)
case class Hair(color: String, hairType: String)

object User {
  implicit val userRW: ReadWriter[User] = macroRW
}

object UserContainer {
  implicit val userContainerRW: ReadWriter[UserContainer] = macroRW
}

object Address {
  implicit val addressRW: ReadWriter[Address] = macroRW
}

object Coordinates {
  implicit val coordinatesRW: ReadWriter[Coordinates] = macroRW
}

object Bank {
  implicit val bankRW: ReadWriter[Bank] = macroRW
}

object Company {
  implicit val companyRW: ReadWriter[Company] = macroRW
}

object Crypto {
  implicit val cryptoRW: ReadWriter[Crypto] = macroRW
}

object Hair {
  implicit val hairRW: ReadWriter[Hair] = macroRW
}