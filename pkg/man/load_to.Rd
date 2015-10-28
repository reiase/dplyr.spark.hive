\name{load_to}
\alias{load_to}
\title{
Create a table from a stored dataset.
}
\description{
Create a table from a stored datasets identified by a URL.
}
\usage{
load_to(dest, data, name, temporary, in.place, ...)
\method{load_to}{src_Hive}(
    dest,
    data,
    name,
    temporary = FALSE,
    in.place = TRUE,
    schema = schema(url),
    ...)
\method{load_to}{src_SparkSQL}(
    dest,
    data,
    name,
    temporary = FALSE,
    in.place = TRUE,
    format = NULL,
    ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{dest}{
  A src object, either src_sparkSQL or src_Hive
}
  \item{data}{
For Hive, a url pointing to the stored data. Which url schemes are supported depends on the installation, but HDFS should be always supported. For Spark, an \code{\link{ExternalData}} object which describes the location and format of the data.
}
  \item{name}{
The name of the table to create}
  \item{temporary}{
Wether the table created should be temporary}
  \item{in.place}{
Whether to move the data to the database own storage or access it in place}
  \item{schema}{
The schema, provided as a named character vector of types (R types) or  a data frame containing a small sample of the data to be loaded.
}
  \item{format}{the format serde to use as a java class}
  \item{\dots}{
Unused, necessary to appease the R gods}
}
\details{
Currently, it can only use the default format which is \code{\001}-separated CSV, no headers, no rownames, no quotes. Format support is on the roadmap. The method for class \code{src_HS2} works for both \code{src_SparkSQL} and \code{src_Hive}. It's a case of documentation-induced implementation detail leakage. The default method just fails with an error message, as \code{load_to} is an API extension specfic to this package and not present in \code{dplyr}.
}
\value{
A tbl for the newly created table}
