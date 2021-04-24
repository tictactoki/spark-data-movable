package com.movable.models

import com.typesafe.config.Config

object NamespaceConfig extends Enumeration {
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

abstract class ServerConfigBuilder(serverName: String) extends ConfigBuilder {
  override def namespace: String = getConfigField(NamespaceConfig.DbsNamespace, serverName)
}

abstract class FileConfigBuilder(inputSource: String) extends ConfigBuilder {
  override def namespace: String = getConfigField(NamespaceConfig.FilesNamespace, inputSource)
}