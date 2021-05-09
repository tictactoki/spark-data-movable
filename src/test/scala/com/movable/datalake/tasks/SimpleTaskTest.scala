package com.movable.datalake.tasks

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.movable.models.{ConfigFactoryTest, Context, FileConfigBuilder}
import com.movable.sessions.{Task, TaskMode}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable


case class SimpleTask(override val config: Config) extends Task[FileConfigBuilder](config) {
  override protected val data: FileConfigBuilder = getFileConfigBuilder(config, "simple_task")

  /**
   *
   * @param context
   * @return the aggregated DataFrame for the specific task
   */
  override def aggregation(context: Context): DataFrame = {
    val options = mutable.HashMap("header"-> "true")
    val input = read(data, session, Some(options))
    input.orderBy("name")
  }
}

class SimpleTaskTest extends AnyFlatSpec with ConfigFactoryTest with SparkSessionTestWrapper with DatasetComparer {


  "A context class" should "get values for default parameters" in {
    val context = new Context()
    assert(context.getMode() === SaveMode.Append)
    assert(context.getPartition() === 8)
  }

  "A context class" should "get the right mode value" in {
    val context = new Context()

    context.update("mode", "lol")
    assert(context.getMode() === SaveMode.Append)

    context.update("mode", TaskMode.Overwrite)
    assert(context.getMode() === SaveMode.Overwrite)

    context.update("mode", TaskMode.Ignore)
    assert(context.getMode() === SaveMode.Ignore)

    context.update("mode", TaskMode.ErrorIfExist)
    assert(context.getMode() === SaveMode.ErrorIfExists)

  }

  "A Simple Task" should "read and aggregate data" in {
    import session.implicits._
    val simpleTask = SimpleTask(config)
    val context = new Context()
    val expected = Seq(("2","Dupont","Dupont"), ("1", "Foo","Bar"), ("3","Odersky","Martin"))
    val expectedDF = expected.toDF("id","name","firstname")
    val df = simpleTask.aggregation(context)

    assertSmallDatasetEquality(df,expectedDF)

  }
}

