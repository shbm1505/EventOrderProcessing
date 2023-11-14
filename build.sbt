ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "EventOrderingProcessor",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0" exclude("org.scala-lang.modules", "scala-xml_2.12"),
      "org.apache.spark" %% "spark-sql" % "3.2.0" exclude("org.scala-lang.modules", "scala-xml_2.12"),
      "com.typesafe" % "config" % "1.4.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0" exclude("org.scala-lang.modules", "scala-xml_2.12"),
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.apache.flink" %% "flink-streaming-scala" % "1.14.0" exclude("org.scala-lang.modules", "scala-xml_2.12"),
      "org.apache.flink" %% "flink-scala" % "1.14.0" exclude("org.scala-lang.modules", "scala-xml_2.12"),
      "org.apache.flink" %% "flink-clients" % "1.14.0",
      "org.apache.flink" % "flink-streaming-java_2.12" % "1.14.0" excludeAll(ExclusionRule(organization = "org.scala-lang")),
      "org.scala-lang.modules" %% "scala-xml" % "2.1.0",
      "org.apache.flink" %% "flink-connector-kafka" % "1.14.0"
    )
  )
