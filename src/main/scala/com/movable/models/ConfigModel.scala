package com.movable.models

class NamespaceConfig extends Enumeration {
  private val Movable = "movable"
  private val Dbs = "dbs"
  private val Files = "files"
  val DbsNamespace = s"$Movable.$Dbs"
  val FilesNamespace = s"$Movable.$Files"
}

trait ConfigNamespace {
  def namespace: String
}

abstract class ConfigBuilder extends ConfigNamespace {
  def getConfigField(namespace: String, field: String) = s"$namespace.$field"
}

