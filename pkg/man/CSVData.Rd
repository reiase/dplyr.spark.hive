\name{CSVData}
\alias{CSVData}
\title{
Describe an external data source for use in \code{\link{load_to}}
}
\description{
Holds location and parsing information for an External data source in CSV format.
}
\usage{
CSVData(url, header = TRUE, delimiter = ",", quote = "\"",
parserLib = c("commons", "univocity"),
            mode = c("PERMISSIVE", "DROPMALFORMED", "FAILFAST"),
charset = "UTF-8", inferSchema = TRUE, comment = "#")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{url}{
A url locating the data set, e.g. \code{hdfs:///data/flights.csv}. Directories and glob patterns are accepted.}
  \item{header}{
Whether the first row contains column names.
}
  \item{delimiter}{
  The field separator.
}
  \item{quote}{
  The quote character. Separators inside quotes are ignored.
}
  \item{parserLib}{
  The underlying parser library to use.
}
  \item{mode}{
Parsing mode can be PERMISSIVE, inserting NAs when parsing is troublesome, DROPMALFORMED, which drops complete rows when encountering problems, or FAILFAST, which fails at the first problem.
}
  \item{charset}{
  Any valid charset name
}
  \item{inferSchema}{
  Whether to try and infer the schema. At this time there is no way to provide a schema with this function, so this has to be TRUE. In the future it may become possible to provide a schema and skip schema inference.
}
  \item{comment}{
Whether to consider lines starting with this character comments, hence skip them.
}
}
\details{
This is a spark-only feature. This capability is not built into spark, but provided by an add-on, spark-csv. Therefore one needs to start the thriftserver with option \code{ --packages com.databricks:spark-csv_<spark-version>:<spark-csv version>}.
}
\value{
An object of type \code{ExternalData} to be passed to \code{load_to}, \code{data} argument.
}
\references{
\url{https://github.com/databricks/spark-csv/blob/master/README.md}
}
