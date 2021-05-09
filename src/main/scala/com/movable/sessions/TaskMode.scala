package com.movable.sessions

object TaskMode extends Enumeration {
  type Mode = String
  val Append: Mode = "append"
  val Overwrite: Mode = "overwrite"
  val ErrorIfExist: Mode = "errorIfExist"
  val Ignore: Mode = "ignore"
}
