package org.delta4test

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class that applies transformation functions one by one on input DataFrame
 */
class DataTransforms(transformations: Seq[DataFrame => DataFrame]) {

  /**
   * Executes functions against DataFrame
   *
   * @param df - input DataFrame against which functions need to be executed
   * @return - modified by Seq of functions DataFrame
   */
  def runTransform(df: DataFrame): DataFrame = transformations.foldLeft(df)((v, f) => f(v))
}
