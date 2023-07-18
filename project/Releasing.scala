import sbt._
import sbt.Keys._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.autoImport.{sonatypeProfileName, sonatypeProjectHosting}

object Releasing {

  lazy val publishSettings: Seq[Def.Setting[_]] = {
    Seq(
      Keys.credentials += Credentials.loadCredentials(Path.userHome / ".artifactory" / "credentials").right.get,
      licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT")),
      homepage := Some(url("https://github.com/mjakubowski84/parquet4s")),
      scmInfo := Some(
        ScmInfo(
          browseUrl  = url("https://github.com/mjakubowski84/parquet4s"),
          connection = "scm:git@github.com:mjakubowski84/parquet4s.git"
        )
      ),
      sonatypeProjectHosting := Some(
        GitHubHosting(user = "mjakubowski84", repository = "parquet4s", email = "mjakubowski84@gmail.com")
      ),
      sonatypeProfileName := "com.github.mjakubowski84",
      developers := List(
        Developer(
          id    = "mjakubowski84",
          name  = "Marcin Jakubowski",
          email = "mjakubowski84@gmail.com",
          url   = url("https://github.com/mjakubowski84")
        )
      ),
      publishMavenStyle := true,
      publishTo := Some(
        "sbtDev" at s"https://tubins.jfrog.io/artifactory/jvm-snapshots;build.timestamp=${new java.util.Date().getTime}"
      ),
      Test / publishArtifact := false,
      IntegrationTest / publishArtifact := false
    ) ++ (if (sys.env contains "SONATYPE_USER_NAME") Signing.signingSettings else Seq.empty)
  }

}
