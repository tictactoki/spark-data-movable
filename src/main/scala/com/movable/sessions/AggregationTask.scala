package com.movable.sessions

import com.movable.models.{DBSConfigBuilder, FileConfigBuilder, FileFormat, RecordConfigBuilder}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.{HashMap, Map}
trait AggregationTask {
  that: MovableSparkSession =>

  protected val outputData: RecordConfigBuilder

  /**
   * Add some specific stuff
   */
  @transient private[sessions] lazy val context: Map[String, String] = HashMap[String, String]()

  /**
   * If you want to add some specific configuration before the run job
   * @param context
   */
  protected def beforeSave(context: Map[String, String]): Unit = {}

  /**
   *
   * @param context
   * @return the aggregated DataFrame for the specific task
   */
  protected def aggregation(context: Map[String, String]): DataFrame

  /**
   * Where you save your data
   * @param context
   * @param dataFrame
   */
  protected def doSave(context: Map[String, String], dataFrame: DataFrame) = {
    outputData match {
      case f: FileConfigBuilder =>
        f.outputFileFormat match {
          case FileFormat.PARQUET => dataFrame.write.parquet(f.outputPath)
          case FileFormat.CSV => dataFrame.write.csv(f.outputPath)
          case FileFormat.JSON => dataFrame.write.json(f.outputPath)
        }
      case d:DBSConfigBuilder => {
        val table = context.getOrElse("table",
          throw new Exception("You have to specify the table where you want to save your data"))
        dataFrame.write.jdbc(d.jdbcUrl, table, d.jdbcProperties)
      }
    }
  }

  /**
   * If you must do something after saving data
   * @param context
   * @param dataFrame
   */
  protected def afterSave(context: Map[String, String], dataFrame: DataFrame): Unit = {}


  def run() = {
    try {
      beforeSave(context)
      val agg = aggregation(context)
      doSave(context, agg)
      afterSave(context, agg)
    } finally {
      session.close()
    }
  }

}
