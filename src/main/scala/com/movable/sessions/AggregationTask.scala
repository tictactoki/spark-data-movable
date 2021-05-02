package com.movable.sessions

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
  protected def doSave(dataFrame: DataFrame) = {}

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
