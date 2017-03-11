lazy val root = (project in file(".")).
	settings(
	name := "amazon_stats",
	scalaVersion := "2.11.1"
)
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.2",
	"org.apache.spark" %% "spark-mllib" % "1.5.2",
	"com.google.code.gson" % "gson" % "1.7.1"
)
