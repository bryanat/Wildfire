
// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.15"
name := "ptwo"
organization := "namenamenamename"
version := "1.0"


lazy val root = project.in(file(".")).
  aggregate(y.js, y.jvm).
  settings(
    publish := {},
    publishLocal := {},
  )
lazy val y = crossProject(JSPlatform, JVMPlatform).in(file(".")).
  settings(
    name := "P1",
    version := "0.1-SNAPSHOT",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    //libraryDependencies += "org.singlespaced" %%% "scalajs-d3" % "0.3.4"
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2",
    libraryDependencies += "io.github.cquiroz" %% "scala-java-time" % "2.2.2"
  ).
  jvmSettings(
    // Add JVM-specific settings here

  ).
  jsSettings(
    // Add JS-specific settings here
    scalaJSUseMainModuleInitializer := true,
    // libraryDependencies += "org.singlespaced" %%% "scala-js-d3" % "0.3.4"
  )
