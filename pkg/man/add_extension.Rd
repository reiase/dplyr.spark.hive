\name{add_extension}
\alias{add_extension}
\alias{add_jar}
\alias{add_function}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Extend backend functionality}
\description{
Both Hive and Spark SQL allow to add user defined functions, variously categorized as UDF, UDAF, UDTF etc. You can now access this capability from R. Once added, you can use these functions in the dots arguments to mutate and summarize.
}
\usage{
add_extension(src, functions, jar)
add_function(src, name, class)
add_jar(src, jar)
}
\arguments{
  \item{src}{
  A Spark_SQL or Hive src object
}
  \item{functions}{
  A character vector who names are function names and values are Java classnames implmenting those functions
}
  \item{jar}{
  A URL to the jar containing necessary class definitions. Must be on a storage accessible to the backend, for instance HDFS or S3
  }
  \item{name}{The name of the function to be defined}
  \item{class}{Full name of the class implmenting the function to be defined}
}
\details{
These functions are used in package tidyr.big, function brickhouse.extension. To see how to write Java classes to extend the backends, google [hive UDF UDAF UDTF] or check out the brickhouse project (http://github.com/klout/brickhouse)

}
\value{
These functions return NULL and are used only for their side effects.
}
