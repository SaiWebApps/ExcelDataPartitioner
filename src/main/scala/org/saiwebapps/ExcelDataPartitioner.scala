package org.saiwebapps

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.functional.RemoteIterators
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._

import org.saiwebapps.SparkExcelFileWriter._

class ExcelDataPartitionerConf(fs: FileSystem, args: Seq[String]) extends ScallopConf(args) {
  final val input_file_path = opt[String]("input_file_path", 'i', required = true, descr = "Input Excel File Path")
  final val tmp_dir_path =
    opt[String]("tmp_dir_path", 't', required = false, descr = "Temporary Directory Path; defaults to ./tmp")
  final val output_file_path = opt[String]("output_file_path", 'o', required = true, descr = "Output Excel File Path")
  final val partition_by = opt[String]("partition_by", 'p', required = true, descr = "Names of columns to partition by")

  def validatePath(path: String, shouldExist: Boolean, shouldBeExcelFile: Boolean): Either[String, Unit] = {
    val fsPath: Path = new Path(path)
    if (shouldExist && !fs.exists(fsPath)) {
      Left(s"$path should exist but does not!")
    } else if (!shouldExist && fs.exists(fsPath)) {
      Left(s"$path should not exist but does!")
    } else if (
      shouldBeExcelFile && !path.endsWith(".xls") && !path.endsWith(".xlsx") &&
      !fs.listLocatedStatus(fsPath).next().isFile()
    ) {
      Left(s"$path should point to an Excel file but does not!")
    } else if (!shouldBeExcelFile && !fs.listLocatedStatus(fsPath).next().isDirectory()) {
      Left(s"$path should point to a directory for storing output Excel files but does not!")
    } else {
      Right()
    }
  }
  validate(input_file_path) { validatePath(_, shouldExist = true, shouldBeExcelFile = true) }
  validate(output_file_path) { validatePath(_, shouldExist = false, shouldBeExcelFile = true) }
  validate(partition_by) { p: String =>
    if (p.trim.split(",").isEmpty) {
      Left("Please specify the names of the columns to partition the data by!")
    } else {
      Right()
    }
  }

  verify()
}

object ExcelDataPartitioner extends App {
  // Initialize Spark resources.
  lazy val spark: SparkSession = SparkSession.builder().master("local[1]").appName(getClass.getSimpleName).getOrCreate()
  lazy val sc: SparkContext = spark.sparkContext
  lazy val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
  lazy val sql: SQLContext = spark.sqlContext

  // Process command-line arguments.
  lazy val cli = new ExcelDataPartitionerConf(fs, args)
  lazy val inputFilePath: String = cli.input_file_path()
  lazy val outputFilePath: String = cli.output_file_path()
  lazy val partitionByColumnNames: Array[String] = cli.partition_by().trim.split(",")
  lazy val tmpDirPath: String = cli.tmp_dir_path.toOption.getOrElse(SparkExcelProperties.DefaultTmpDirPath)
  lazy val tmpDirFsPath: Path = new Path(tmpDirPath)

  def run(): Unit = {
    try {
      println(s"Loading $inputFilePath....")
      val sparkExcelFileReader = SparkExcelFileReader(sql)
      val inputDf: DataFrame = sparkExcelFileReader.read(inputFilePath, partitionByColumnNames: _*)

      println(s"Partitioning by ${partitionByColumnNames.mkString("[", ",", "]")} to $tmpDirPath....")
      inputDf.writeExcelPartitions(tmpDirPath, partitionByColumnNames: _*)

      println("Initializing Excel sheets....")
      val partitionedFileFsPaths: Seq[Path] =
        RemoteIterators.toList(fs.listFiles(tmpDirFsPath, /*recursive=*/ true)).asScala.toSeq.map(_.getPath)
      val sheetNamesAndDataFilePaths: Seq[(String, String)] = partitionedFileFsPaths
        .flatMap { p: Path =>
          p.getParent.toString.split("=") match {
            case Array(_, companyName, _*) => Option((SparkExcelFileWriter.prepareSheetName(companyName), p.toString))
            case _                         => None
          }
        }
        .sortBy { case (sheetName: String, _) => sheetName }
      val totalNumSheets: Int = sheetNamesAndDataFilePaths.size

      println("Populating Excel sheets....")
      val startTime: Long = System.nanoTime()
      print(s"0 / $totalNumSheets processed....")
      sheetNamesAndDataFilePaths.zipWithIndex.foreach { case ((sheetName: String, sheetDataPath: String), i: Int) =>
        sparkExcelFileReader.read(sheetDataPath).appendToExcelSheet(outputFilePath, sheetName)
        print(s"\r${i + 1} / $totalNumSheets processed....")
      }
      val endTime: Long = System.nanoTime()
      val elapsed: Double = (endTime - startTime) / 1e9
      println(s"\nDONE in $elapsed seconds!")
    } finally {
      // Cleanup
      if (fs.exists(tmpDirFsPath)) {
        fs.delete(tmpDirFsPath, /*recursive=*/ true)
      }
    }
  }

  run()
}
