package org.delta4test

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.io.Directory

//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs._


case class Album(albumId: Long, title: String, tracks: Array[String], updateDate: Long)
case class AlbumAlterCol(albumId: Long, title: String, tracks: Array[String], updateDate: Long, isPub: Boolean)
case class AlbumTypeChange(albumId: Long, title: Long, tracks: Array[String], updateDate: Long)
case class orderEvents(id: String, eventTime: String)
/**
 * Spark job aimed at testing basic features of Delta Lake.
 */
object DeltaLakeTest {

  /**
   * Initial DF
   */
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def dateToLong(dateString: String): Long = LocalDate.parse(dateString, formatter).toEpochDay

  private val INITIAL_ALBUM_DATA = Seq(
    Album(800, "old 1 6 String Theory", Array("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01")),
    Album(801, "old 2 Hail to the Thief", Array("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
//    Album(801, "old 2 Hail to the Thief", Array("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
    Album(802, "old 3 804", Array("2+2=5", "Backdrifts", "Go to sleep"), dateToLong("2019-12-01"))
  )
  private val INITIAL_EVENT_DATA = Seq(
    orderEvents("9", "yyyy-MM-dd HH:mm:ss.S")
  )
  private val UPSERT_ALBUM_DATA = Seq(
//    Album(800, "new 1 6 String Theory - Special", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-03"))
//    ,Album(801, "new 2 Special dupkey", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-03"))
//    ,Album(802, "new 3 new Best Of Jazz Blues", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-04"))
//    ,Album(802, "new 3 new Best Of Jazz Blues", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-04"))
    Album(803, "new 4 Birth of Cooloverwrite", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
//    ,Album(803, "new 4 Birth of Cooloverwrite", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
//    Album(803, "Birth of Cooloverwrite", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
  )
  private val ALTER_COL_DATA = Seq(
    AlbumAlterCol(803, "Birth of Cool", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-04"), true)
  )
  private val TYPE_CHANGE_DATA = Seq(
    AlbumTypeChange(800, 10000, Array("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01"))
  )

  //  val basePath = "hdfs://localhost:9000/delta_test"
//  val basePath = "/tmp/delta_test"
  val basePath = "/tmp/deltaLakeCacheZone"



  def main(args: Array[String]): Unit = {
//    clearDirectory()
    val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("DeltaLakeTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // Display versions
    val versionScala: String = util.Properties.versionString
    val versionSpark: String = spark.version
    println("Spark: " + versionSpark + "  -  Scala: " + versionScala)

    // Create a table
    // Store the data into Delta Lake
    import spark.implicits._
//    val tableName = "delta_users2"


    val tableName = "orderEvents"
//    upsert(INITIAL_EVENT_DATA.toDF(), tableName, "overwrite")
    query(spark, tableName)

//    upsert(INITIAL_ALBUM_DATA.toDF(), tableName, "overwrite")
//    alterTests(spark, basePath, tableName)
//    query(spark, tableName)

    /**
    query(spark, tableName)
    upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "append")
    query(spark, tableName)
    duplicateMerge(spark, tableName, UPSERT_ALBUM_DATA.toDF())
    query(spark, tableName)

    upsert(INITIAL_ALBUM_DATA.toDF(), tableName)

    // Read (latest version of the) data from Delta Lake
    query(spark, tableName)

    // Store the updated version of the data into Delta Lake
    upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "overwrite")
    query(spark, tableName)

    // Read older versions of data from Delta Lake using time travel
    timeTravelQuery(spark, basePath, tableName)

    mergeTests(spark, basePath, tableName)
    query(spark, tableName)

    alterTests(spark, basePath, tableName)
    query(spark, tableName)

    // latest version query
    latestQuery(spark, tableName)
    **/

//    upsert(TYPE_CHANGE_DATA.toDF(), tableName, "overwrite")
//    query(spark, tableName)

//    largeScaleDataTesting(spark, tableName="events_table", sourceFilePath="/tmp/source_data/pre_events.csv")
//    largeScaleDataTesting(spark, tableName="events_table", sourceFilePath="/tmp/source_data/events.csv")
    // End of the Spark session
    spark.stop()
  }

  private def clearDirectory(): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()
  }

  def upsert(df: DataFrame, tableName: String): Unit = {
    df.write.format("delta").save(s"$basePath/$tableName/")
  }

  def upsert(df: DataFrame, tableName: String, mode: String): Unit = {
    df.write.format("delta").mode(mode).save(s"$basePath/$tableName/")
  }

  def upsert(df: DataFrame, tableName: String, mode: String, partitionByKey: String): Unit = {
    df.write.format("delta").partitionBy(partitionByKey).mode(mode).save(s"$basePath/$tableName/")
  }

  def query(spark: SparkSession, tableName: String) = {
    val df = spark.read
      .format("delta")
      .option("mergeSchema", "true")
      .load(s"$basePath/$tableName")
    df.show(false)
    df.printSchema()
    Thread.sleep(10000)
  }

  def timeTravelQuery(spark: SparkSession, basePath: String, tableName: String) = {
    val historyDF = spark.read
      .format("delta")
      .option("versionAsOf", 0)
      .load(s"$basePath/$tableName")
    historyDF.show()
    Thread.sleep(10000)
  }

  def duplicateMerge(spark: SparkSession, tableName: String, newDf: DataFrame) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val deltaTable: DeltaTable = io.delta.tables
      .DeltaTable
      .forPath(s"$basePath/$tableName")

    deltaTable
      .as("logs")
      .merge(
        newDf.as("newDedupedLogs"),
        "logs.albumId= newDedupedLogs.albumId")
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    deltaTable.toDF.show(100, false)

  }
  def mergeTests(spark: SparkSession, basePath: String, tableName: String) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    // Merge tests
    val deltaTable: DeltaTable = io.delta.tables
      .DeltaTable
      .forPath(s"$basePath/$tableName")

    deltaTable.toDF.show(100, false);

    deltaTable.update(
      condition = col("albumId") === 800,
      set = Map("title" -> lit("6 String Theory Updated"))
    )
    deltaTable.toDF.show(100, false);

    //    deltaTable.delete(
    //      condition = col("albumId") === 800
    //    )
    //    deltaTable.toDF.show(100, false);

    // Upsert (merge) new data
    val newData = UPSERT_ALBUM_DATA.toDF()

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.albumId = newData.albumId")
      .whenMatched
      .updateExpr(
        Map(
          "title" -> "newData.title",
          "tracks" -> "newData.tracks",
          "updateDate" -> "newData.updateDate"
        )
      )
      .whenNotMatched
      .insertExpr(
        Map(
          "title" -> "newData.title",
          "albumId" -> "newData.albumId",
          "tracks" -> "newData.tracks",
          "updateDate" -> "newData.updateDate"
        )
      )
      .execute()

    println("After conditional merge:")
    deltaTable.toDF.show(100, false)


    // History
    //    val fullHistoryDF = deltaTable.history()
    //    println ("History of table updates:")
    //    fullHistoryDF.show()

  }

  def alterTests(spark: SparkSession, basePath: String, tableName: String) = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val deltaTable: DeltaTable = io.delta.tables
      .DeltaTable
      .forPath(s"$basePath/$tableName")

    deltaTable.toDF.printSchema()

//    val newData = ALTER_COL_DATA.toDF()
    val newData = TYPE_CHANGE_DATA.toDF()
    newData.write.format("delta")
//      .option("mergeSchema", "true")
      .option("overwriteSchema", "true")
      .mode("append")
      .save(s"$basePath/$tableName")

    newData.show(100, false)

  }

  def latestQuery(spark: SparkSession, tableName: String) = {

  }

  def largeScaleDataTesting(spark: SparkSession, tableName: String, sourceFilePath: String) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val eventsDF = spark
      .read
      .option("header", "true")
      .csv(sourceFilePath)
    eventsDF.printSchema()

    upsert(
      eventsDF.withColumn("updateDate", $"event_time".substr(0, 10)),
      tableName,
      "overwrite",
      "updateDate"
    )

    query(spark, tableName)
  }

}
