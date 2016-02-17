# Copyright 2015 Revolution Analytics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

convert.from.DB =
  function(type) {
    switch(
      tolower(type),
      tinyint = as.integer,
      smallint = as.integer,
      int = as.integer,
      bigint = as.numeric,
      boolean = as.logical,
      float = as.double,
      double = as.double,
      string = as.character,
      binary = as.raw,
      timestamp = as.POSIXct,
      decimal = as.double,
      date = as.Date,
      varchar = as.character,
      char = as.character,
      stop("Don't know what to map ", type, " to"))}

# this uses  a compute then fetch approach to be able to query the result schema
# and do type conversions, it's a work around for some type blunder in fetch
collect.tbl_HS2 =
  function(x, ...) {
    x = compute(x, temporary = FALSE)
    res = dplyr:::collect.tbl_sql(x, ...)
    db.types =
      DBI::dbGetQuery(x$src$con, paste("describe", x$from))$data_type
    db_drop_table(table = paste0('`', x$from,'`'), con = x$src$con)
    sapply(
      seq_along(res),
      function(i)
        res[[i]] <<- convert.from.DB(db.types[i])(res[[i]]))
    res}

head.tbl_HS2 =
  function(x, n = 6L, ...) {
    x$query = dplyr:::build_query(x, as.integer(n))
    collect.tbl_HS2(x)}

#modeled after mutate_ methods in http://github.com/hadley/dplyr,
#under MIT license
# this does multiple partial_evals to allow new cols to be used to define new cols
# not supported directly in HS2
# then avoids duplicate col names to avoid the wrong selection in transmute
# finally adds a collapse when win funs are present
mutate_.tbl_HS2 =
  function (.data, ..., .dots) {
    dots = all_dots(.dots, ..., all_named = TRUE)
    input = partial_eval_mod(dots, .data)
    for(i in 1:length(input))
      input = partial_eval_mod(input, .data, input)
    .data$mutate = TRUE
    no.dup.select = .data$select[!as.character(.data$select) %in% names(input)]
    new = update(.data, select = c(no.dup.select, input))
    collapse(new) }

#modeled after filter_ methods in http://github.com/hadley/dplyr,
#under MIT license
# This adds parens to clarify priority in cascaded filters
# adds collapse when new cols used in filter
filter_.tbl_HS2 =
  function (.data, ..., .dots)   {
    dots = all_dots(.dots, ...)
    lapply(seq_along(dots), function(i) dots[[i]]$expr <<- call("(", dots[[i]]$expr))
    input = partial_eval(dots, .data)
    if(
      any(
        names(.data$select) %in%
        flatten(all.vars(input[[1]]))))
      .data = collapse(.data)
    update(.data, where = c(.data$where, input))}

assert.compatible =
  function(x, y)
    if(suppressWarnings(!all(colnames(x) == colnames(y))))
      stop("Tables not compatible")

#modeled after union methods in http://github.com/hadley/dplyr,
#under MIT license
# HS2 uses UNION ALL
union.tbl_HS2 =
  function (x, y, copy = FALSE, ...) {
    assert.compatible(x, y)
    y = dplyr:::auto_copy(x, y, copy)
    sql = sql_set_op(x$src$con, x, y, "UNION ALL")
    distinct(dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = groups(x)))}

#modeled after intersect methods in http://github.com/hadley/dplyr,
#under MIT license
# intersect simulated with an inner_join and select for lack of native support
intersect.tbl_HS2 =
  function (x, y, copy = FALSE, ...){
    assert.compatible(x, y)
    xy = inner_join(x, y, copy = copy)
    select_(xy, .dots = setNames(colnames(xy)[1:(NCOL(xy)/2)], colnames(x)))}

#modeled after join methods in http://github.com/hadley/dplyr,
#under MIT license
# this looks like it's converged back to the dplyr original
some_join =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ..., type) {
    by = dplyr:::common_by(by, x, y)
    y = dplyr:::auto_copy(x, y, copy, indexes = if (auto_index)
      list(by$y))
    sql = dplyr::sql_join(x$src$con, x, y, type = type, by = by)
    dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = groups(x))}

# missing dplyr feature
right_join.tbl_HS2 =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ...) {
    some_join(x = x, y = y, by = by, copy = copy, auto_index = auto_index, ..., type = "right")}

#missing dplyr feature
full_join.tbl_HS2 =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ...) {
    some_join(x = x, y = y, by = by, copy = copy, auto_index = auto_index, ..., type = "full")}

# refresh.tbl_sql =
#   function(x, src = refresh(x$src), ...) {
#     tbl(src, x$query$sql)}

cache =
  function(tbl) {
    DBI::dbSendQuery(tbl$src$con, paste("CACHE TABLE", tbl$from))
    tbl}
