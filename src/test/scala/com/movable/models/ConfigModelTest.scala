package com.movable.models

import com.movable.models.dbs.{DBSDriver, DBSType}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec

trait ConfigFactoryTest {
  val config = ConfigFactory.load()
}

class ConfigModelTest extends AnyFlatSpec with ConfigFactoryTest {

  "A File Config Builder class" should "load the config in FileConfigBuilder format" in {
    val configBuilder = FileConfigBuilder(config, "localhost")
    assert(configBuilder.outputFileFormat === "parquet")
    assert(configBuilder.outputPath === "output_path")
    assert(configBuilder.inputFileFormat === Some("csv"))
    assert(configBuilder.inputPath === Some("input_path"))
  }

  "A File Config Builder class" should "load the config in FileConfigBuilder format without input data" in {
    val configBuilder = FileConfigBuilder(config, "without_input")
    assert(configBuilder.outputFileFormat === "parquet")
    assert(configBuilder.outputPath === "output_path")
    assert(configBuilder.inputFileFormat === None)
    assert(configBuilder.inputPath === None)
  }

  "A DBS Config Builder class" should "load the config in DBSConfigBuilder format" in {
    val configBuilder = DBSConfigBuilder(config, "localhost", "dbs_games")
    assert(configBuilder.driver === DBSDriver.PsqlDriver)
    assert(configBuilder.baseType === DBSType.PostgreSQL)
    assert(configBuilder.port === Some(5432))
    assert(configBuilder.pwd === None)
  }

  "An AWS Config Builder class" should "load the config in AWSConfigBuilder format" in {
    val configBuilder = AWSConfigBuilder(config)
    assert(configBuilder.region === "eu-west-3")
    assert(configBuilder.getBucket("datalake_team1") === Some("s3://bucket-team1"))
    assert(configBuilder.getBucket("datalake_not_exist_team") === None)
  }

}
