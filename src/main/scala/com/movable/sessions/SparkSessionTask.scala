package com.movable.sessions

import com.movable.models.SparkConfigBuilder
import com.movable.utils.S3Utils
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

abstract class SparkSessionTask(config: Config) {

  val s3Utils = S3Utils(config)
  protected val sparkConfigBuilder = SparkConfigBuilder(config)
  protected val isLocal = sparkConfigBuilder.isLocalJob

  lazy val builder: Boolean => SparkSession.Builder = (isLocal: Boolean) => {
    if (isLocal)
      SparkSession.builder().master("local[*]")
    else SparkSession.builder()
  }

  lazy val session: SparkSession = {
    val sc = builder(isLocal).getOrCreate()
    if(isLocal) {
      sc.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",s3Utils.env.getCredentials.getAWSAccessKeyId)
      sc.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",s3Utils.env.getCredentials.getAWSSecretKey)
    }
    sc
  }
  lazy val sc: SparkContext = session.sparkContext
}