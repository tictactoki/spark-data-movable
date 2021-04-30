package com.movable.models

import com.movable.sessions.SparkSessionTask

trait SparkSessionConnection {
  that: SparkSessionTask =>

  protected val getJdbcConfigBuilder = (serverName: String, dbs: String) => DBSConfigBuilder(that.config, serverName, dbs)
  protected val getFileConfigBuilder = (inputPath: String) => FileConfigBuilder(that.config, inputPath)



}
