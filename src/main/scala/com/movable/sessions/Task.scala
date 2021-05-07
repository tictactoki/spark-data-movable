package com.movable.sessions

import com.movable.models.RecordConfigBuilder
import com.typesafe.config.Config

abstract class Task[T <: RecordConfigBuilder](override val config: Config)
  extends MovableSparkSession(config) with SparkSessionConnection with AggregationTask[T]
