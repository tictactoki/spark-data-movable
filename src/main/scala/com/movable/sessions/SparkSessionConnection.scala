package com.movable.sessions

import com.movable.models.NamespaceConfig.DBSNamespace.{Driver, Pwd, Username}
import com.movable.models.{ConfigBuilder, DBSConfigBuilder, FileConfigBuilder, FileFormat}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

trait SparkSessionConnection {
  that: MovableSparkSession =>

  protected val getJdbcConfigBuilder = (config: Config, serverName: String, dbs: String) =>
    DBSConfigBuilder(config, serverName, dbs)
  protected val getFileConfigBuilder = (config: Config, inputPath: String) => FileConfigBuilder(config, inputPath)

  protected val inputData: ConfigBuilder
  protected val outputData: ConfigBuilder

  protected def read(fileConfigBuilder: FileConfigBuilder, session: SparkSession): DataFrame = {
    val optDf = for {
      inputFormat <- fileConfigBuilder.inputFileFormat
      inputPath <- fileConfigBuilder.inputPath
    } yield {
      inputFormat match {
        case FileFormat.CSV => session.read.csv(inputPath)
        case FileFormat.JSON => session.read.json(inputPath)
        case FileFormat.PARQUET => session.read.parquet(inputPath)
      }
    }
    optDf.getOrElse(session.emptyDataFrame)
  }

  protected def read(dbsConfigBuilder: DBSConfigBuilder,
                     session: SparkSession)(table: String): DataFrame = {
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
