package com.movable.models

import com.typesafe.config.Config

trait SparkSessionConnection {

  protected val getJdbcConfigBuilder = (config: Config, serverName: String, dbs: String) =>
    DBSConfigBuilder(config, serverName, dbs)
  protected val getFileConfigBuilder = (config: Config, inputPath: String) => FileConfigBuilder(config, inputPath)



}
