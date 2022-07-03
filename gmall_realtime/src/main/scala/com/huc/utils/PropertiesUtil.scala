package com.huc.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertieName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }

  //  def main(args: Array[String]): Unit = {
  //    val properties: Properties = load("config.properties")
  //    val str: String = properties.getProperty("kafka.broker.list")
  //    println(str)
  //  }
  def main(args: Array[String]): Unit = {

  }
}
