/** [[https://monix.io]] */
val MonixVersion = "3.1.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.10",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "filestream",
    libraryDependencies ++= Seq(
      "io.monix" %% "monix" % MonixVersion
    )
  )
