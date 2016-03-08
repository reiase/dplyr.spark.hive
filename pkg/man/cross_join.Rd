\name{cross_join}
\alias{cross_join}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Perform a cross join of two tables}
\description{
An addition to the several join functions in dplyr, this function performs a cross join of two tables, or a cartesian product.
}
\usage{
cross_join(x, y, by = NULL, copy = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x, y}{ tables to be joined}
  \item{by}{ if different from NULL, \code{cross_join} becomes like an \code{inner_join}}
  \item{copy}{If x an y are not from the same data source and copy is TRUE, y will be copied to the source of x. Untested and unlikely to work.}
  \item{\dots}{Method-specific parameters}
}
\details{
Remeber to estimate the size of the resulting table before using this function.
}
\value{
A table of the same class as x
}
