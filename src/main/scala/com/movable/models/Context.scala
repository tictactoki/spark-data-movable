package com.movable.models

import scala.collection.mutable
import scala.util.Try

class Context extends mutable.HashMap[String, String] {

  def getValueAsInt(key: String, default: Int): Int = {
    Try(get(key).map(_.toInt)).toOption.flatten.getOrElse(default)
  }

  def getValueAsLong(key: String, default: Long): Long = {
    Try(get(key).map(_.toLong)).toOption.flatten.getOrElse(default)
  }

  def getValueAsDouble(key: String, default: Double): Double = {
    Try(get(key).map(_.toDouble)).toOption.flatten.getOrElse(default)
  }


}
