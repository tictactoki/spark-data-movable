package com.movable.sessions

import com.movable.models.{DBSConfigBuilder, FileConfigBuilder, FileFormat}
import org.apache.spark.sql.DataFrame

trait AggregationTask {
  that: MovableSparkSession =>

  /**
   * If you want to add some specific configuration before the run job
   */
  protected def beforeSave(): Unit = {}

  /**
   *
   * @return the aggregated DataFrame for the specific task
   */
  protected def aggregation(): DataFrame

  /**
   * Where you save your data
   *
   * @param dataFrame
   */
  protected def doSave(dataFrame: DataFrame) = {
    outputData match {
      case f: FileConfigBuilder =>
        f.outputFileFormat match {
          case FileFormat.PARQUET => dataFrame.write.parquet(f.outputPath)
          case FileFormat.CSV => dataFrame.write.csv(f.outputPath)
          case FileFormat.JSON => dataFrame.write.json(f.outputPath)
        }
      //case d:DBSConfigBuilder => dataFrame.write.jdbc(d.jdbcUrl,)
      case _ => throw new Exception
    }
  }

  /**
   * If you must do something after saving data
   *
   * @param dataFrame
   */
  protected def afterSave(dataFrame: DataFrame): Unit = {}


  def run() = {
    try {
      beforeSave()
      val agg = aggregation()
      doSave(agg)
      afterSave(agg)
    } finally {
      session.close()
    }
  }

}
