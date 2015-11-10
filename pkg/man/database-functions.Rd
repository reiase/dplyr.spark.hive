\name{database.functions}
\alias{database.functions}
\title{
Supported database functions
}
\description{
Both Spark and hive support a large number of functions that you can use when writing dplyr expressions. Here you find a list and some suggestions as to how to use them.
}

\details{
Database functions, in this context, are any functions that one can use in a dplyr expression, e.g. \code{mutate(a.table, f(a.column))}. These are executed in the database and, for this package, by Spark or Hive. dplyr supports them via two mechanisms, translation and pass-through. R functions that are translated to an equivalent or approximately equivalent database function can be listed with \code{src_translate_env(src)}, returning a list of function names or \code{map(flatten(map(src_translate_env(my_db), as.list)), args)
} returning all the signatures. If dplyr doesn't have any translation information for a function, it passes it as is to the backend, so the user can use any supported database function, with an exception for those with a special syntax, such as casts, that need to be translated. For additional information, see:
\enumerate{
\item{the \href{Spark DataFrame function reference}{https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions\%24}, which is a scala function reference, for lack of a dedicated SQL document}
\item{the oddly named \href{HiveQL Language Manual UDF}{https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF}, which uses the moniker "user defined" for builtins, but seems to be otherwise correct and should largely be relevant for both Spark and Hive}
\item{the \href{dplyr vignette about databases}{https://cran.r-project.org/web/packages/dplyr/vignettes/databases.html}, specifically the SQL translation section}
}
}
