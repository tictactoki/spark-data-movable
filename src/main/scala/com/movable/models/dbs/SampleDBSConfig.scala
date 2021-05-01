package com.movable.models.dbs

import com.movable.models.{DBSConfigBuilder, SparkSessionConnection}
import com.typesafe.config.Config

case class SampleDBSConfig(config: Config) extends SparkSessionConnection {

  protected lazy val jdbcConfigBuilder = DBSConfigBuilder(config, "sampleServer", "sampledbs")




}
