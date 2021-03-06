import sbtassembly.PathList

//sbt clean assembly
//dse spark-submit --class DataLoader doc-assembly-0.1.jar 1000 1000000 20
//dse spark-submit --executor-memory=6g --class BenchmarkJoins doc-assembly-0.1.jar 3
//dse spark-submit --executor-memory=6g --conf spark.dse.continuous_paging_enabled=true --class BenchmarkJoins doc-assembly-0.1.jar 3
//dse spark-submit --executor-memory=6g --class BenchmarkSpaceEfficiency doc-assembly-0.1.jar 3
//dse spark-submit --executor-memory=6g --class BenchmarkDataframe doc-assembly-0.1.jar 3
//dse spark-submit --executor-memory=6g --class SimpleSparkStreaming doc-assembly-0.1.jar localhost 9999 2 false true


version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"
//val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7", // % "provided",

  "org.apache.spark" %% "spark-core"      % sparkVersion, // % "provided",
  "org.apache.spark" %% "spark-sql"       % sparkVersion, // % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion, // % "provided",
  "com.thedeanda" % "lorem" % "2.1"

)



// resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
}



