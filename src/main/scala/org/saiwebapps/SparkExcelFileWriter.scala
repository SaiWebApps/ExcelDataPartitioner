package org.saiwebapps

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

case object SparkExcelFileWriter {
  implicit class PartitionWriter(df: DataFrame) {
    def writeExcelPartitions(dirPath: String, partitionByColumnNames: String*): Unit = {
      df.write
        .format(SparkExcelProperties.ExcelOutputDirFormat)
        .partitionBy(partitionByColumnNames: _*)
        .mode("overwrite")
        .save(dirPath)
    }
  }

  implicit class FileWriter(df: DataFrame) {
    def appendToExcelSheet(
        filePath: String,
        sheetName: String,
        options: Map[String, String] = Map("header" -> "true")
    ): Unit = {
      writeToExcelFile(filePath, Map("dataAddress" -> sheetName) ++ options, "append")
    }

    def writeToExcelFile(filePath: String, options: Map[String, String], mode: String): Unit = {
      options
        .foldLeft(df.write.format(SparkExcelProperties.ExcelOutputFileFormat)) {
          case (acc: DataFrameWriter[Row], (optionKey: String, optionValue: String)) =>
            acc.option(optionKey, optionValue)
        }
        .mode(mode)
        .save(filePath)
    }
  }

  def prepareSheetName(v: String): String = s"'$v'!A1"
}
