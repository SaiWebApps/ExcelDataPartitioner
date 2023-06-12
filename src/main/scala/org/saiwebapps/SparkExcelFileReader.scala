package org.saiwebapps

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, isnan, isnull}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class SparkExcelFileReader(
    sqlContext: SQLContext,
    options: Map[String, String] = Map("header" -> "true", "inferSchema" -> "true")
) {
  val reader: DataFrameReader = options.foldLeft(sqlContext.read.format(SparkExcelProperties.ExcelOutputFileFormat)) {
    case (acc: DataFrameReader, (optionName: String, optionValue: String)) => acc.option(optionName, optionValue)
  }

  def read(filePath: String, partitionByColumnNames: String*): DataFrame = {
    val rawDf: DataFrame = reader.load(filePath).cache()
    val orderedDf: DataFrame = if (partitionByColumnNames.nonEmpty) {
      rawDf.orderBy(partitionByColumnNames.map(col): _*)
    } else {
      rawDf
    }
    dropEmptyColumns(orderedDf)
  }

  private def dropEmptyColumns(df: DataFrame): DataFrame = {
    val emptyColumns: Seq[String] = df.columns.filter { columnName: String =>
      df.filter(!isnan(col(columnName)) && !isnull(col(columnName))).isEmpty
    }
    df.drop(emptyColumns: _*)
  }
}
