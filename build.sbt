name := "ParQR (Parallel Qualitative Reasoner)"

version := "1.0"

scalaVersion := "2.11.8"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
    