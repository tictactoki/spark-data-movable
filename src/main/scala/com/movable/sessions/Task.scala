package com.movable.sessions

import com.typesafe.config.Config

abstract class Task(override val config: Config)
  extends MovableSparkSession(config) with SparkSessionConnection with AggregationTask
