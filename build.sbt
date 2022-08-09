name := "SparkSandBox"

version := "0.1"

scalaVersion := "2.12.14"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"

libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.7.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.23"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.2.5"
libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.13.0"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.2"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.13.0"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0"
libraryDependencies += "io.circe" %% "circe-parser" % "0.10.1"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.4.2"