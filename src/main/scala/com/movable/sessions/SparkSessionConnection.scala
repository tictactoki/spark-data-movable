package com.movable.sessions

import com.movable.models.{ConfigBuilder, DBSConfigBuilder, FileConfigBuilder, FileFormat, RecordConfigBuilder}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkSessionConnection {
  that: MovableSparkSession =>

  protected type C = RecordConfigBuilder

  protected val getJdbcConfigBuilder = (config: Config, serverName: String, dbs: String) =>
    DBSConfigBuilder(config, serverName, dbs)
  protected val getFileConfigBuilder = (config: Config, inputPath: String) => FileConfigBuilder(config, inputPath)

  protected val inputData: C
  protected val outputData: C

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
    session.read.jdbc(dbsConfigBuilder.jdbcUrl, table, dbsConfigBuilder.jdbcProperties)
  }


}
