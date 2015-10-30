\name{JDBCData}
\alias{JDBCData}
%- Also NEED an '\alias' for EACH other topic documented here.

\title{
Describe an external data source for use in \code{\link{load_to}}
}
\description{
Holds information about an external data sources held in a database that supports a JDBC server (e.g. postgresql). This is quite untested, use with discretion.
}
\usage{
JDBCData(url, dbtable, driver, partitionColumn = NULL, lowerBound = NULL,
         upperBound = NULL, numPartitions = NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{url}{
The urls of the JDBC server}
  \item{dbtable}{
The name of the table to be read}
  \item{driver}{
The class name of the JDBC driver}
  \item{partitionColumn}{
Describes how to partition the table, must be a numeric column
}
  \item{lowerBound, upperBound, numPartitions}{
Hints to the partitioning process}
}
\details{
The last four options need to be all set, or none of them. Given that I haven't found very good documentation about them yet, I'd say skip for now.
For sqlite, a driver could be "org.sqlite.JDBC" and the jar from the \href{https://bitbucket.org/xerial/sqlite-jdbc}{xerial} project "sqlite-jdbc-<version>.jar". You can specify the additional jar one the cmd line of the spark thrift server with --jars (comma separated list). That said, I am still having problem with this setup so your milage may vary, please report back.
}
\value{
An object of type \code{ExternalData} to be passed to \code{load_to}, \code{data} argument.

}
\references{
\url{https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases}}
