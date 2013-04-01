package scalding

import sbt._
import Keys._
import sbtassembly.Plugin._
import sbtgitflow.ReleasePlugin._
import com.github.bigtoast.sbtthrift.ThriftPlugin._

object MyBuild extends Build {


  val customThriftSettings = Seq(

    thrift := "/usr/local/bin/thrift",

    thriftJavaOptions := Seq( "hashcode", "java5" )
  )

  object Versions{
    val scala = "2.10.0"
    val lift = "2.5-RC1"
    val hadoop = "2.0.0-cdh4.2.0"
    val scalaIO = "0.4.2"
  }

  /** */
  val sharedSettings = Defaults.defaultSettings ++ Project.defaultSettings ++ assemblySettings ++ releaseSettings ++ Seq(

    organization := "com.github.berngp",

    scalaVersion := Versions.scala,

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases" at "http://oss.sonatype.org/content/repositories/releases",
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "Concurrent Maven Repo" at "http://conjars.org/repo"
    ),


    //TODO issolate a per Project lib Seq sharing a common Seq.
    libraryDependencies ++= Seq(
      "org.apache.commons" % "commons-lang3" % "3.1",
      "com.github.scala-incubator.io" %% "scala-io-core" % Versions.scalaIO,
      "com.github.scala-incubator.io" %% "scala-io-file" % Versions.scalaIO,
      "net.liftweb" %% "lift-util" % Versions.lift % "compile",
      "org.apache.hadoop" % "hadoop-client" % Versions.hadoop,
      "org.apache.hadoop" % "hadoop-yarn-client" % Versions.hadoop,
      "org.scalaz" %% "scalaz-core" % "6.0.4",
      "org.apache.thrift" % "libthrift" % "0.9.0" % "compile",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.specs2" %% "specs2" % "1.14" % "test",
      "org.mockito" % "mockito-core" % "1.9.5" % "test"
    ),
    parallelExecution in Test := false,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

    pomExtra := (
      <url>https://github.com/berngp/thrift-scala-example</url>
        <licenses>
          <license>
            <name>Apache 2</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
            <comments>A business-friendly OSS license</comments>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:berngp/thrift-scala-example.git</url>
          <connection>scm:git:git@github.com:twitter/thrift-scala-example.git</connection>
        </scm>
        <developers>
          <developer>
            <id>berngp</id>
            <name>Bernardo Gomez Palacio</name>
            <url>http://twitter.com/berngp</url>
          </developer>
        </developers>)
  )

  /** */
  lazy val myProject = Project(
    id = "root",
    base = file("."),
    settings = sharedSettings
  ).aggregate(thriftSchemaPrj, producerPrj, consumerPrj)

  /** */
  lazy val thriftSchemaPrj = Project(
    id = "thrift-schema",
    base = file("thrift-schema")
  ).settings( sharedSettings : _* )
    .settings( thriftSettings : _* )
    .settings( customThriftSettings : _*)
    .configs(Thrift)

  /** */
  lazy val producerPrj = Project(
    id = "producer",
    base = file("producer"),
    settings = sharedSettings
  ).dependsOn(thriftSchemaPrj)

  /** */
  lazy val consumerPrj = Project(
    id = "consumer",
    base = file("consumer"),
    settings = sharedSettings
  ).dependsOn(thriftSchemaPrj)

}
