package com.movable.sessions

import com.movable.models.{Context, DBSConfigBuilder, FileConfigBuilder, FileFormat, RecordConfigBuilder}
import org.apache.spark.sql.{DataFrame, SaveMode}

trait AggregationTask[T <: RecordConfigBuilder] {
  that: MovableSparkSession =>

  protected val data: T

  /**
   * Add some specific stuff
   */
  @transient private[sessions] lazy val context: Context = {
    val context = new Context()
    context.initDefault()
    context
  }

  /**
   * Add value in context
   * @param context
   */
  protected def initContext(context: Context) {}

  /**
   * If you want to add some specific configuration before the run job
   * @param context
   */
  protected def beforeSave(context: Context): Unit = {}

  /**
   *
   * @param context
   * @return the aggregated DataFrame for the specific task
   */
  def aggregation(context: Context): DataFrame

  /**
   * Where you save your data
   * @param context
   * @param dataFrame
   */
  protected def doSave(context: Context, dataFrame: DataFrame) = {
    val mode = context.getMode()
    data match {
      case f: FileConfigBuilder =>
        f.outputFileFormat match {
          case FileFormat.PARQUET => dataFrame.write.mode(mode).parquet(f.outputPath)
          case FileFormat.CSV => dataFrame.write.mode(mode).csv(f.outputPath)
          case FileFormat.JSON => dataFrame.write.mode(mode).json(f.outputPath)
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
  protected def afterSave(context: Context, dataFrame: DataFrame): Unit = {}


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
