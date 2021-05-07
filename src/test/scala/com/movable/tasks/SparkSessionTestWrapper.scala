package com.movable.tasks

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  @transient lazy val session: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
