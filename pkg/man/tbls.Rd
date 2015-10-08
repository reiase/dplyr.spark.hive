\name{tbls}
\alias{tbls}
\title{
Create variables and assign them with tbls for each table in a database
}
\description{
Tired of typing \code{tblname = tbl(src, "tblname")} for each table in a database? This command achieves the same with less typing.
}
\usage{
tbls(src, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{src}{ A src object, either a src_SparkSQL or a src_Hive object.
}
  \item{\dots}{
Useless, just to appease the R gods.
}
}
\details{
  Variables are created in the frame of the caller.
}
\value{
Invisibly, a list of the objects created.
}
