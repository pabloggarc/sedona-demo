ThisBuild / scalaVersion := "2.13.17"

// Spark and Sedona are already on the cluster, so the jar only holds the classes of the jobs
lazy val root = (project in file("."))
  .settings(
    name := "sedona-jobs",
    version := "0.1.0",
    Compile / packageBin / artifactPath := target.value / "sedona-jobs.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.1.1" % Provided,
      "org.apache.sedona" % "sedona-spark-shaded-4.1_2.13" % "1.9.1" % Provided
    )
  )
