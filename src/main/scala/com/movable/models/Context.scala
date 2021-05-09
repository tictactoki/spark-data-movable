package com.movable.models

import com.movable.sessions.TaskMode
import org.apache.spark.sql.SaveMode

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

  def getMode(): SaveMode = {
    get(ContextDefaultKey.Mode).map(Context.getMode).getOrElse(SaveMode.Append)
  }

  def getPartition(): Int = {
    getValueAsInt(ContextDefaultKey.PartitionNumber, 8)
  }

}

object Context {
  import TaskMode._
  def getMode(mode: TaskMode.Mode) = mode match {
    case Overwrite => SaveMode.Overwrite
    case ErrorIfExist => SaveMode.ErrorIfExists
    case Ignore => SaveMode.Ignore
    case _ => SaveMode.Append
  }

}

object ContextDefaultKey {
  val Mode = "mode"
  val PartitionNumber = "partition"
  val PartitionType = "partitionType"
}