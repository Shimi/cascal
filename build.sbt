name := "cascal"

version := "1.3-SNAPSHOT"

organization := "com.shorrockin"

scalaVersion := "2.10.1"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
    "org.apache.cassandra" % "cassandra-all" %  "1.1.10",
    "org.apache.cassandra" % "cassandra-thrift" % "1.1.10",
	"com.eaio.uuid" % "uuid" % "3.2",
	"org.slf4j" % "slf4j-api" % "1.6.4",
	"commons-pool" % "commons-pool" % "1.6",
	"junit" % "junit" % "4.10" % "test"
  )
	
libraryDependencies += "com.novocode" % "junit-interface" % "0.7" % "test->default"

publishTo <<= (version) { version: String =>
      Some(Resolver.file("file", new File("/home/shimi/workspace/shimi-mvn-repo/") / {
        if  (version.trim.endsWith("SNAPSHOT"))  "snapshots"
        else                                     "releases/" }    ))
}

scalacOptions ++= Seq("-feature", "-Xlog-implicits")
