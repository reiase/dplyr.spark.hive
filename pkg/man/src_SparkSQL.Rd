\name{src_SparkSQL}
\alias{src_SparkSQL}
\alias{src_Hive}

\title{
Create a dplyr src backed by SparkSQL or Hive
}
\description{
Create a dplyr src backed by SparkSQL or Hive. Backends support most operations other databases do with the familiar dplyr syntax and semantics, with high scalability.
}
\usage{
src_SparkSQL(
  host =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"),
     "localhost"),
  port =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_PORT"),
      10000),
  start.server = FALSE,
  server.opts = list())
src_Hive(
  host =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"),
      "localhost"),
  port =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_PORT"),
      10000))
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{host}{
 The address of the host running the thift server (see Details)
}
  \item{port}{ The port on which the thirft server is listening}
  \item{start.server}{Whether to start the Spark thrift server, defaults to using one if already running, starting one otherwise. Requires SPARK_HOME to be set}
  \item{server.opts}{Server options, as a named list, will be added to command to start the Spark thrift server, see \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server}}
}

\details{
If you already have a thrift server running (see \url{https://spark.apache.org/docs/latest/sql-programming-guide.html} for spark or \url{https://cwiki.apache.org/confluence/display/Hive/HiveServer} for Hive), you just need to set host and port. For Spark only, there is the ability to start a local server.
}
\value{
An object of class \code{src_SparkSQL} or \code{src_Hive}. Use it like any other src of the SQL variety in dplyr.
}


\examples{
   #See the \code{dplyr} tutorial.
}
