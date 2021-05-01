package com.movable.models

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import NamespaceConfig.DBSNamespace._

import java.util.Properties

trait SparkSessionConnection {

  protected val getJdbcConfigBuilder = (config: Config, serverName: String, dbs: String) =>
    DBSConfigBuilder(config, serverName, dbs)
  protected val getFileConfigBuilder = (config: Config, inputPath: String) => FileConfigBuilder(config, inputPath)

  protected def read(fileConfigBuilder: FileConfigBuilder, session: SparkSession): DataFrame = {
    fileConfigBuilder.inputFileFormat match {
      case FileFormat.CSV => session.read.csv(fileConfigBuilder.inputPath)
      case FileFormat.JSON => session.read.json(fileConfigBuilder.inputPath)
      case FileFormat.PARQUET => session.read.parquet(fileConfigBuilder.inputPath)
    }
  }

  protected def read(dbsConfigBuilder: DBSConfigBuilder,
                     session: SparkSession,
                     table: String): DataFrame = {
    val properties = {
      val p = new Properties()
      p.setProperty(Username, dbsConfigBuilder.username)
      p.setProperty(Driver, dbsConfigBuilder.driver)
      dbsConfigBuilder.pwd.map { pwd => p.setProperty(Pwd, pwd) }
      p
    }
    session.read.jdbc(dbsConfigBuilder.jdbcUrl, table, properties)
  }


}