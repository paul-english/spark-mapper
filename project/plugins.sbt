resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("net.pishen" % "sbt-emr-spark" % "0.6.1")

resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
  url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
  Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.1.1")

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("au.com.onegeek" %% "sbt-dotenv" % "1.1.36")
