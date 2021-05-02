package com.movable.models

import com.movable.models.dbs.{DBSDriver, DBSType}
import com.typesafe.config.Config

import java.util.Properties
import scala.util.Try

object NamespaceConfig extends Enumeration {
  val Movable = "movable"
  val Dbs = "dbs"
  val Files = "files"
  val Spark = "spark"
  val Aws = "aws"

  val DbsNamespace = s"$Movable.$Dbs"
  val FilesNamespace = s"$Movable.$Files"

  object DBSNamespace {
    val Driver = "driver"
    val Host = "host"
    val Db = "db"
    val Port = "port"
    val Username = "username"
    val Pwd = "password"
    val Type = "type"
  }

  object FileNamespace {
    val InputFileFormat = "input_file_format"
    val InputPath = "input_path"
    val OutputPath = "output_path"
  }

  object SparkNamespace {
    val IsLocalJob = "is_local_job"
    val WorkerNumber = "worker_number"
  }

  object AwsNamespace {
    val Region = "region"
  }

}

trait ConfigNamespace {
  val namespace: String
}

sealed abstract class ConfigBuilder(val config: Config) extends ConfigNamespace {
  def getConfigField(namespace: String)(field: String) = s"$namespace.$field"
  protected lazy val curryingNamespace: String => String = (field: String) => getConfigField(namespace)(field)
}

sealed trait RecordConfigBuilder

final case class SparkConfigBuilder(override val config: Config) extends ConfigBuilder(config) {
  import NamespaceConfig.SparkNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.Movable)(NamespaceConfig.Spark)
  lazy val isLocalJob = config.getBoolean(getConfigField(namespace)(IsLocalJob))
  lazy val workerNumber = Try(config.getString(getConfigField(namespace)(WorkerNumber))).getOrElse("*")
}

final case class AWSConfigBuilder(override val config: Config) extends ConfigBuilder(config) {
  import NamespaceConfig.AwsNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.Movable)(NamespaceConfig.Aws)
  lazy val region = config.getString(getConfigField(namespace)(Region))
}

final case class DBSConfigBuilder(override val config: Config, serverName: String, dbs: String)
  extends ConfigBuilder(config) with RecordConfigBuilder {
  import NamespaceConfig.DBSNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.DbsNamespace)(serverName)
  lazy val dbsNamespace: String = curryingNamespace(dbs)
  //lazy val driver: Option[String] = Try(config.getString(s"$dbsNamespace.$Driver")).toOption
  lazy val host: String = config.getString(s"$dbsNamespace.$Host")
  lazy val port: Option[Int] = Try(config.getInt(s"$dbsNamespace.$Port")).toOption
  lazy val db: String = config.getString(s"$dbsNamespace.$Db")
  lazy val username: String = config.getString(s"$dbsNamespace.$Username")
  lazy val pwd: Option[String] = Try(config.getString(s"$dbsNamespace.$Pwd"))toOption
  lazy val baseType: String = config.getString(s"$dbsNamespace.$Type")
  lazy val (jdbcUrl, driver) = {
    val p = port.map(i => s":$i").getOrElse("")
    baseType match {
      case DBSType.SQLServer => (s"jdbc:sqlserver://$host$p;databaseName=$db", DBSDriver.SqlServerDriver)
      case DBSType.MySQL => (s"jdbc:mysql://$host$p/$db", DBSDriver.MysqlDriver)
      case DBSType.PostgreSQL => (s"jdbc:postgresql://$host$p/$db", DBSDriver.PsqlDriver)
    }
  }
  lazy val jdbcProperties = {
    val p = new Properties()
    p.setProperty(Username, username)
    p.setProperty(Driver, driver)
    pwd.map { pwd => p.setProperty(Pwd, pwd) }
    p
  }
}

final case class FileConfigBuilder(override val config: Config,
                                 inputDirectorySource: String)
  extends ConfigBuilder(config) with RecordConfigBuilder {
  import NamespaceConfig.FileNamespace._
  override val namespace: String = getConfigField(NamespaceConfig.FilesNamespace)(inputDirectorySource)
  lazy val fileNamespace: String => String = getConfigField(namespace)
  lazy val inputFileFormat: Option[String] = Try(fileNamespace(InputFileFormat)).toOption
  lazy val inputPath: Option[String] = Try(fileNamespace(InputPath)).toOption
  lazy val outputFileFormat: String = fileNamespace(InputFileFormat)
  lazy val outputPath: String = fileNamespace(outputPath)
}