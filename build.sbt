
name := "spark-poc"

version := "1.0.0"


scalaVersion := "2.10.6"

scalacOptions ++= Seq("-feature")

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-sql" % "1.6.1",
    "com.github.nscala-time" %% "nscala-time" % "1.8.0",
    "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.19",
    "org.jpmml" % "pmml-evaluator" % "1.3.3"
  )


resolvers ++= Seq(
  "Twitter Repository" at "http://maven.twttr.com" // For hadoop-lzo
)
