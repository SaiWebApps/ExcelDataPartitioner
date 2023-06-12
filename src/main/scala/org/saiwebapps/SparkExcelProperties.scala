package org.saiwebapps

case object SparkExcelProperties {
  /**
   * Default temp directory path.
   */
  final val DefaultTmpDirPath = "tmp"

  /**
   * To read from / write to a single Excel file.
   */
  final val ExcelOutputFileFormat = "com.crealytics.spark.excel"

  /**
   * To write multiple Excel files to a directory.
   */
  final val ExcelOutputDirFormat = "excel"
}
