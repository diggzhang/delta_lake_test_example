package org.delta4test


import java.util.Properties

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.JavaConverters._

/**
 * Class that contains JDBC source, read parallelism params and target table name
 *
 *  @param source       - JDBC source table
 *  @param destination  - Delta target database.table
 *  @param splitBy      - column by which to split source data while reading
 *  @param chunks       - to how many chunks split jdbc source data
 */
case class ImportConfig(source: String, destination: String, splitBy: String, chunks: Int, partitionBy: Tuple2[String, String]) {
  val bounds_sql = s"""
  (select min($splitBy) as lower_bound, max($splitBy) as upper_bound from $source) as bounds
  """
}

/**
 * Class that does reading from JDBC source, transform and writing to Delta table
 *
 *  @param jdbcUrl       - url connecting string for jdbc source
 *  @param importConfig  - case class that contains source read parallelism params and target table
 *  @param jdbcParams    - additional JDBC session params like isolation level, perf tuning,
 *                       net wait params etc...
 *  @param dataTransform - contains function that we should apply to transform our source data
 */
class JDBCImport(jdbcUrl: String,
                 importConfig: ImportConfig,
                 jdbcParams: Map[String, String] = Map(),
                 dataTransform: DataTransforms)
                (implicit val spark: SparkSession) {

  import spark.implicits._

  implicit def mapToProperties(m: Map[String, String]): Properties = {
    val properties = new Properties()
    m.foreach(pair => properties.put(pair._1, pair._2))
    properties
  }

  // list of columns to import is obtained from schema of destination delta table
  private lazy val targetColumns = DeltaTable
    .forPath(importConfig.destination)
    .toDF
    .schema
    .fieldNames

  private lazy val sourceDataframe = readJDBCSourceInParallel()
//    .select(targetColumns.map(col): _*)

  /**
   * obtains lower and upper bound of source table and uses those values to read in a JDBC dataframe
   * @return a dataframe read from source table
   */
  private def readJDBCSourceInParallel(): DataFrame = {

    val (lower, upper) = spark
      .read
      .jdbc(jdbcUrl, importConfig.bounds_sql, jdbcParams)
      .as[(Option[Long], Option[Long])]
      .take(1)
      .map { case (a, b) => (a.getOrElse(0L), b.getOrElse(0L)) }
      .head

    spark.read.jdbc(
      jdbcUrl,
      importConfig.source,
      importConfig.splitBy,
      lower,
      upper,
      importConfig.chunks,
      jdbcParams)
  }

  private implicit class DataFrameExtensionOps(df: DataFrame) {

    def runTransform(): DataFrame = dataTransform.runTransform(sourceDataframe)

    def writeToDelta(importConfig: ImportConfig): Unit = df
      .write
      .format("delta")
      .option("mergeSchema", "true")
      .mode(SaveMode.Overwrite)
      .partitionBy(importConfig.partitionBy._2)
      .save(importConfig.destination)

    def mergeToDelta(importConfig: ImportConfig) = {
      val deltaTable: DeltaTable = io.delta.tables
        .DeltaTable
        .forPath(importConfig.destination)

      deltaTable.as("old")
        .merge(
           df.as("new"),
          "old.id=new.id"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }
  }

  /**
   * Runs transform against dataframe read from jdbc and writes it to Delta table
   */
  def run(): Unit = {
    sourceDataframe
      .runTransform()
      .mergeToDelta(importConfig)
//      .writeToDelta(importConfig)
  }
}

object JDBCImport {
  def apply(jdbcUrl: String,
            importConfig: ImportConfig,
            jdbcParams: Map[String, String] = Map(),
            dataTransforms: DataTransforms = new DataTransforms(Seq.empty))
           (implicit spark: SparkSession): JDBCImport = {

    new JDBCImport(jdbcUrl, importConfig, jdbcParams, dataTransforms)
  }
}
