\name{ExternalData}
\alias{ExternalData}

\title{
Describe an external data source for use in \code{\link{load_to}}
}
\description{
Holds information about an external data source.
}
\usage{
ExternalData(parser, options)
}
\arguments{
  \item{parser}{
  A parser class.
}
  \item{options}{
Options accepted by that parser class.
}
}
\details{
This is for users to create their own custom data sources. For CSV use \code{\link{CSVData}} and for JDBC use \code{\link{JDBCData}} instead.
}
\value{
An object of type \code{ExternalData} to be passed to \code{load_to}, \code{data} argument.
}
