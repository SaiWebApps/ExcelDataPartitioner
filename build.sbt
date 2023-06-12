version := "1.0.0"
organization := "org.saiwebapps"
scalaVersion := "2.12.15"

lazy val hadoopVersion = "3.3.5"
lazy val sparkVersion = "3.3.1"
libraryDependencies ++= Seq(
  "com.crealytics" %% "spark-excel" % s"${sparkVersion}_0.18.7",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.rogach" %% "scallop" % "4.1.0"
)
