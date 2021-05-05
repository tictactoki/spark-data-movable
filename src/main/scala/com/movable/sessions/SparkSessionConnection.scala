package com.movable.sessions

import com.movable.models.{ConfigBuilder, DBSConfigBuilder, FileConfigBuilder, FileFormat, RecordConfigBuilder}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkSessionConnection {
  that: MovableSparkSession =>

  protected val getJdbcConfigBuilder = (config: Config, serverName: String, dbs: String) =>
    DBSConfigBuilder(config, serverName, dbs)
  protected val getFileConfigBuilder = (config: Config, inputPath: String) => FileConfigBuilder(config, inputPath)

  protected def read(fileConfigBuilder: FileConfigBuilder, session: SparkSession,
                     optOptions: Option[Map[String, String]] = None): DataFrame = {
    val optDf = for {
      inputFormat <- fileConfigBuilder.inputFileFormat
      inputPath <- fileConfigBuilder.inputPath
    } yield {
      val reader = optOptions.map { options => session.read.options(options) }.getOrElse(session.read)
      inputFormat match {
        case FileFormat.CSV => reader.csv(inputPath)
        case FileFormat.JSON => reader.json(inputPath)
        case FileFormat.PARQUET => reader.parquet(inputPath)
      }
    }
    optDf.getOrElse(session.emptyDataFrame)
  }

  protected def read(dbsConfigBuilder: DBSConfigBuilder,
                     session: SparkSession)(table: String): DataFrame = {
    session.read.jdbc(dbsConfigBuilder.jdbcUrl, table, dbsConfigBuilder.jdbcProperties)
  }


}
