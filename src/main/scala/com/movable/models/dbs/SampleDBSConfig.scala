package com.movable.models.dbs

import com.movable.models.DBSConfigBuilder
import com.typesafe.config.Config

case class SampleDBSConfig(config: Config) {

  protected lazy val inputData = DBSConfigBuilder(config, "localhost", "games")


}
