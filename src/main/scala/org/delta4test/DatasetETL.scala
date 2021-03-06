package org.delta4test


import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.rogach.scallop.{CliOption, ScallopConf, ScallopOption}


object DatasetETL {


  def main(args: Array[String]): Unit = {

    implicit val spark = org.apache.spark.sql.SparkSession
      .builder()
      .appName("DeltaLakeTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val deltaLakeBasePath = "/tmp/deltaLakeCacheZone"
    val tableName = "orderEvents"
    val jdbcUrl = "jdbc:mysql://localhost:3306/etlSource?user=root&password=debezium"

    val config = ImportConfig(
      source = tableName,
      destination = s"$deltaLakeBasePath/$tableName",
      splitBy = "id",
      chunks = 10,
      partitionBy = ("eventTime"->"partitionTs")
    )

    // define a transform to convert all timestamp columns to strings
    val timeStampsToStrings : DataFrame => DataFrame = source => {
      val tsCols = source.schema.fields.filter(_.dataType == DataTypes.TimestampType).map(_.name)
      tsCols.foldLeft(source)((df, colName) =>
        df.withColumn(colName, from_unixtime(unix_timestamp(col(colName)), "yyyy-MM-dd HH:mm:ss.S")))
    }

    // Whatever functions are passed to below transform will be applied during import
    val transforms = new DataTransforms(Seq(
      df => df.withColumn("id", col("id").cast(types.StringType)) // cast id column to string
      ,df => df.withColumn("partitionTs", col(config.partitionBy._1).substr(0, 10))
      ,timeStampsToStrings // use transform defined above for timestamp conversion
    ))

    new JDBCImport(jdbcUrl = jdbcUrl, importConfig = config, dataTransform = transforms)
      .run()
    query(spark, config.destination)


    spark.close()
  }

  def query(spark: SparkSession, destination: String) = {
    val df = spark.read
      .format("delta")
      .option("mergeSchema", "true")
      .load(s"$destination")
    df.show(false)
    df.printSchema()
  }

}
