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

# both following function taken from dplyr with minor changes
# to compensate for unsupport SQL variations
#
over  =
  function (expr, partition = NULL, order = NULL, frame = NULL)  {
    args = (!is.null(partition)) + (!is.null(order)) + (!is.null(frame))
    if (args == 0) {
      stop("Must supply at least one of partition, order, frame",
           call. = FALSE) }
    if (!is.null(partition)) {
      partition =
        build_sql(
          "PARTITION BY ",
          sql_vector(partition, collapse = ", ",  parens = FALSE))}
    if (!is.null(order)) {
      order = build_sql("ORDER BY ", sql_vector(order, collapse = ", ", parens = FALSE))}
    if (!is.null(frame)) {
      if (is.numeric(frame))
        frame = rows(frame[1], frame[2])
      frame = build_sql("ROWS ", frame) }
    over =
      sql_vector(
        compact(list(partition, order, frame)),
        parens = TRUE)
    build_sql(expr, " OVER ", over)}

environment(over) = environment(select_)

partial_eval_mod =
  function (call, tbl = NULL, env = parent.frame())
  {
    message("in modded version")
    if (is.atomic(call))
      return(call)
    if (inherits(call, "lazy_dots")) {
      lapply(call, function(l) partial_eval_mod(l$expr, tbl, l$env))
    }
    else if (is.list(call)) {
      lapply(call, partial_eval_mod, tbl = tbl, env = env)
    }
    else if (is.symbol(call)) {
      name <- as.character(call)
      if (!is.null(tbl) && name %in% tbl_vars(tbl)) {
        call
      }
      else if (exists(name, env)) {
        eval(call, env)
      }
      else {
        call
      }
    }
    else if (is.call(call)) {
      name <- as.character(call[[1]])
      if (name == "local") {
        eval(call[[2]], env)
      }
      else if (name %in% c("$", "[[")) {
        eval(call, env)
      }
      else {
        call[-1] <- lapply(call[-1], partial_eval_mod, tbl = tbl,
                           env = env)
        call
      }
    }
    else {
      stop("Unknown input type: ", class(call), call. = FALSE)
    }
  }

default_op_mod =
  function (x)
  {
    assert_that(is.string(x))
    sql_sq_bracket =
      function(){
        function(x, y) {
          build_sql(x, "[", as.integer(y), "]")
        }
      }
    infix <- c("::", "$", "@", "^", "*", "/", "+", "-", ">",
               ">=", "<", "<=", "==", "!=", "!", "&", "&&", "|", "||",
               "~", "<-", "<<-")
    if (x == "[") {
      sql_sq_bracket()
    }
    else if (x %in% infix) {
      sql_infix(x)
    }
    else if (grepl("^%.*%$", x)) {
      x <- substr(x, 2, nchar(x) - 1)
      sql_infix(x)
    }
    else {
      sql_prefix(x)
    }
  }

environment(default_op_mod) = environment(select_)

.onLoad = function(lib, pkg) {
  assign(
    'n_distinct',
    function(x) {
      build_sql("COUNT(DISTINCT ", x, ")")},
    envir = base_agg)
  utils::assignInNamespace(
    x = "over",
    ns = "dplyr",
    value = over)
  utils::assignInNamespace(
    x = "partial_eval",
    ns = "dplyr",
    value = partial_eval_mod)
  utils::assignInNamespace(
    x = "default_op",
    ns = "dplyr",
    value = default_op_mod)
  # doesn't seem necessary anymore
  #  assignInNamespace(
  #     "unique_name",
  #     function()
  #       paste0("tmp", strsplit(as.character(runif(1)), "\\.")[[1]][2]),
  #     ns = "dplyr")
}
