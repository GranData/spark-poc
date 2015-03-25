name := "spark-poc"

version := "1.0.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"

libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"

mainClass in assembly := Some("poc.Main")