package com.movable.models.dbs

import com.movable.models.DBSConfigBuilder
import com.movable.sessions.{MovableSparkSession, SparkSessionConnection}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

abstract class SQLReader(override val config: Config)
  extends MovableSparkSession(config) with SparkSessionConnection {

  protected val inputData: DBSConfigBuilder
  protected val getTable: String => DataFrame = read(inputData, session)

}
