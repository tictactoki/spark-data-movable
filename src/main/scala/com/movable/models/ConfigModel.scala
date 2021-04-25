package com.movable.models

import com.typesafe.config.Config

import scala.util.Try

object NamespaceConfig extends Enumeration {
  private val Movable = "movable"
  private val Dbs = "dbs"
  private val Files = "files"
  val DbsNamespace = s"$Movable.$Dbs"
  val FilesNamespace = s"$Movable.$Files"


  object DBSNamespace {
    val Driver = "driver"
    val Host = "host"
    val Db = "db"
    val Port = "port"
    val Username = "username"
    val Pwd = "password"
  }

  object FileNamespace {
    val InputFileFormat = "input_file_format"
    val InputPath = "input_path"
    val OutputPath = "output_path"
  }

}

trait ConfigNamespace {
  val namespace: String
}

abstract class ConfigBuilder extends ConfigNamespace {
  def getConfigField(namespace: String)(field: String) = s"$namespace.$field"
  protected lazy val curryingNamespace: String => String = (field: String) => getConfigField(namespace)(field)
}

case class DBSConfigBuilder(config: Config, serverName: String, dbs: String) extends ConfigBuilder {
  import NamespaceConfig.DBSNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.DbsNamespace)(serverName)
  lazy val dbsNamespace: String = super.curryingNamespace(dbs)
  lazy val driver: String = config.getString(s"$dbsNamespace.$Driver")
  lazy val host: String = config.getString(s"$dbsNamespace.$Host")
  lazy val port: Try[Int] = Try(config.getInt(s"$dbsNamespace.$Port"))
  lazy val db: String = config.getString(s"$dbsNamespace.$Db")
  lazy val username: Try[String] = Try(config.getString(s"$dbsNamespace.$Username"))
  lazy val pwd: Try[String] = Try(config.getString(s"$dbsNamespace.$Pwd"))
}

case class FileConfigBuilder(config: Config,
                                 inputDirectorySource: String) extends ConfigBuilder {
  import NamespaceConfig.FileNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.FilesNamespace)(inputDirectorySource)
  lazy val fileNamespace: String => String = getConfigField(namespace)
  lazy val inputFileFormat: String = fileNamespace(InputFileFormat)
  lazy val inputPath: String = fileNamespace(InputPath)
  lazy val outputPath: String = fileNamespace(outputPath)
}