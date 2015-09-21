name := "spark-poc"

version := "1.0.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-feature")

libraryDependencies ++=
  Seq("org.apache.spark" %% "spark-core" % "1.3.0",
"org.apache.spark" %% "spark-sql" % "1.3.0",
"com.github.nscala-time" %% "nscala-time" % "1.8.0",
    "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.19")


resolvers ++= Seq(
  "Twitter Repository" at "http://maven.twttr.com" // For hadoop-lzo
)
