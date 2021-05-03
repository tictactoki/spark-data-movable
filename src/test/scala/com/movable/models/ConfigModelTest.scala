package com.movable.models

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

}
