import sbt.Def.settings
import sbtassembly.AssemblyPlugin.assemblySettings

name := "BigDataProject"

version := "0.1"

scalaVersion := "2.12.12"

mainClass in Compile := Some("com.kuznetsov.Main")

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.0.1"

lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.kuznetsov",
  scalaVersion := "2.12.12",
  test in assembly := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("Main")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "project.jar"

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value

//e
//
