name := "modou-dataset-example"

scalaVersion := "2.11.12"
version := "0.0.2-spark2.4"

crossScalaVersions := Seq("2.11.12", "2.12.8")

checksums in update := Nil

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"
libraryDependencies += "org.specs2" %% "specs2-core" % "4.7.0" % "test"
libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"
libraryDependencies += "org.antlr" % "antlr-runtime" % "3.5.2"
libraryDependencies += "org.apache.xbean" % "xbean-asm6-shaded" % "4.10"
libraryDependencies += "io.delta" %% "delta-standalone" % "0.2.0"
libraryDependencies += "org.rogach" %% "scallop" % "3.5.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"


// Spark
libraryDependencies ++= (version.value match {
  case v if v.contains("spark2.3") => Seq(
    "org.apache.spark" %% "spark-core" % "2.3.2",
    "org.apache.spark" %% "spark-sql" % "2.3.2",
    "org.apache.spark" %% "spark-mllib" % "2.3.2",
    "org.apache.spark" %% "spark-hive" % "2.3.2"
  )
  case v if v.contains("spark2.4") => Seq(
    "org.apache.spark" %% "spark-core" % "2.4.3",
    "org.apache.spark" %% "spark-sql" % "2.4.3",
    "org.apache.spark" %% "spark-mllib" % "2.4.3",
    "org.apache.spark" %% "spark-hive" % "2.4.3"
  )
  case _ => Seq()
})

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}

javacOptions in Compile ++= Seq("-source", "1.8",  "-target", "1.8")

scalacOptions ++= Seq("-deprecation", "-feature")

// Hortonworks HDP Hive 3 (having a static method in an interface)
scalacOptions ++= (version.value match {
  case v if v.contains("hive3hdp") => Seq("-target:jvm-1.8")
  case _ => Seq()
})

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishConfiguration := publishConfiguration.value.withOverwrite(true)

publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

cleanKeepFiles += target.value / "test-reports"
