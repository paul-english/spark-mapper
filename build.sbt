name := "spark-mapper"

version := "1.0.0"

organization := "com.github.log0ymxm"

homepage := Some(url("https://github.com/log0ymxm/spark-mapper"))

licenses += ("Apache", url("https://github.com/log0ymxm/spark-mapper/blob/master/LICENSE"))

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.specs2" %% "specs2-core" % "3.8.7" % "test"
)

javaOptions ++= Seq("-Xms512M", "-Xmx8192M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

fork in Test := true

scalacOptions in Test ++= Seq("-Yrangepos")

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _ *) => MergeStrategy.last
  case PathList("org", "apache", xs @ _ *) => MergeStrategy.last
  case "overview.html" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

scmInfo := Some(ScmInfo(url("https://github.com/log0ymxm/spark-mapper"), "git@github.com:log0ymxm/spark-mapper.git"))

publishMavenStyle := true

publishArtifact in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomIncludeRepository := { _ => false }

pomExtra :=
  <developers>
    <developer>
      <id>log0ymxm</id>
      <name>Paul English</name>
      <url>http://github.com/log0ymxm</url>
    </developer>
  </developers>
