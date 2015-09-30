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

setClass(
  "HS2Connection",
  contains = "JDBCConnection")

db_list_tables.HS2Connection =
  function(con)
    dbGetQuery(con, "show tables")[,1]

db_has_table.HS2Connection =
  function(con, table)
    table %in% db_list_tables(con)

db_query_fields.HS2Connection =
  function(con, sql){
    map(
      strsplit(
        x =
          names(
            dbGetQuery(
              con,
              build_sql("SELECT * FROM ", sql, " LIMIT 0", con = con))),
        split = "\\."),
      tail,
      1)}


db_explain.HS2Connection = dplyr:::db_explain.MySQLConnection

db_begin.HS2Connection =
  function(con, ...) TRUE

db_commit.HS2Connection =
  function(con, ...) TRUE

db_rollback.HS2Connection =
  function(con, ...) TRUE

db_drop_table.HS2Connection =
  function (con, table, force = FALSE, ...) {
    sql =
      build_sql(
        "DROP TABLE ",
        if(force) sql("IF EXISTS "),
        ident(table),
        con = con)
    RJDBC::dbSendUpdate(con, sql)}

db_data_type.HS2Connection = dplyr:::db_data_type.DBIConnection

setMethod(
  "dbDataType",
  signature = "HS2Connection",
  function(dbObj, obj, ...)
    switch(
      class(obj)[[1]],
      character = "STRING",
      Date =    "DATE",
      factor =  "STRING",
      integer = "INT",
      logical = "BOOLEAN",
      numeric = "DOUBLE",
      POSIXct = "TIMESTAMP",
      raw = "BINARY",
      stop(
        "Can't map",
        paste(class(obj), collapse = "/"),
        "to a supported type")))

#modeled after db_insert_into methods in http://github.com/hadley/dplyr,
#under MIT license
db_insert_into.HS2Connection =
  function (con, table, values, ...) {
    cols = lapply(values, escape, collapse = NULL, parens = FALSE,
                  con = con)
    col_mat = matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))
    rows = apply(col_mat, 1, paste0, collapse = ", ")
    values = paste0("(", rows, ")", collapse = "\n, ")
    sql = build_sql("INSERT INTO TABLE", ident(table), " VALUES ",
                    sql(values),con = con)
    RJDBC::dbSendUpdate(con, sql)}

db_analyze.HS2Connection =
  function(con, table, ...) TRUE

db_create_index.HS2Connection =
  function(con, table, columns, name = NULL, ...)
    TRUE

db_create_table.HS2Connection =
  function(con, table, types, temporary = TRUE, url = NULL, ...) {
    external = !is.null(url)
    table = tolower(table)
    stopifnot(is.character(table) && length(table) == 1)
    stopifnot(is.character(types))
    field_names =
      escape(
        ident(names(types)),
        collapse = NULL,
        con = con)
    fields =
      dplyr:::sql_vector(
        paste0(field_names, " ", types),
        parens = TRUE,
        collapse = ", ",
        con = con)
    sql =
      build_sql(
        "CREATE ",
        if(external) sql("EXTERNAL "),
        if(temporary) sql("TEMPORARY "),
        "TABLE ", ident(table), " ",
        fields,
        if(external) build_sql(sql(" LOCATION "), encodeString(url)),
        con = con)
    RJDBC::dbSendUpdate(con, sql)}

db_save_query.HS2Connection =
  function(con, sql, name, temporary = TRUE, ...){
    name = tolower(name)
    if(temporary)
      stop("Compute into temporary not supported yet. Set temporary = FALSE")
    dplyr:::db_save_query.DBIConnection(con, sql, name, temporary, ...)}

db_explain.HS2Connection =
  function(con, sql, ...)
    dbGetQuery(
      con,
      build_sql("EXPLAIN ", sql))

db_load_table =
  function(con, table, url) {
    st =
      build_sql(
        "LOAD DATA INPATH ",
        encodeString(url),
        " INTO TABLE ",
        ident(table),
        con = con)
    RJDBC::dbSendUpdate(con = con, statement = st)
    invisible()}

sql_escape_string.HS2Connection =
  function(con, x)
    sql_quote(x, "'")

sql_escape_ident.HS2Connection =
  function(con, x)
    sql_quote(tolower(x), "`")

#modeled after sql_join methods in http://github.com/hadley/dplyr,
#under MIT license
sql_join.HS2Connection =
  function (con, x, y, type = "inner", by = NULL, ...) {
    join =
      switch(
        type,
        left = sql("LEFT"),
        inner = sql("INNER"),
        right = sql("RIGHT"),
        full = sql("FULL"),
        stop("Unknown join type:", type, call. = FALSE))
    by = common_by(by, x, y)
    x_names = auto_names(x$select)
    y_names = auto_names(y$select)
    uniques = unique_names(x_names, y_names, NULL)
    if(is.null(uniques)) {
      sel_vars = c(x_names, y_names)}
    else {
      x = update(x, select = setNames(x$select, uniques$x))
      y = update(y, select = setNames(y$select, uniques$y))
      by$x = unname(uniques$x[by$x])
      by$y = unname(uniques$y[by$y])
      sel_vars = unique(c(uniques$x, uniques$y))}
    name.left = unique_name()
    name.right = unique_name()
    on =
      sql_vector(
        paste0(
          paste(sql_escape_ident(con, name.left),sql_escape_ident(con, by$x), sep = "."),
          " = ",
          paste(sql_escape_ident(con, name.right), sql_escape_ident(con, by$y), sep = "."),
          collapse = " AND "),
        parens = TRUE)
    cond = build_sql("ON ", on, con = con)
    from =
      build_sql(
        "SELECT * FROM ", sql_subquery(con, x$query$sql, name.left),
        "\n\n", join, " JOIN \n\n", sql_subquery(con, y$query$sql, name.right),
        "\n\n", cond, con = con)
    attr(from, "vars") = lapply(sel_vars, as.name)
    from}

environment(sql_join.HS2Connection) = environment(select_)

#modeled after sql_semi_join methods in http://github.com/hadley/dplyr,
#under MIT license
sql_semi_join.HS2Connection =
  function (con, x, y, anti = FALSE, by = NULL, ...){
    if(anti) stop("antijoins not implemented yet")
    by = dplyr:::common_by(by, x, y)
    left = dplyr::escape(ident("_LEFT"), con = con)
    right = dplyr::escape(ident("_RIGHT"), con = con)
    on =
      dplyr:::sql_vector(
        paste0(
          left, ".", dplyr::sql_escape_ident(con, by$x), " = ",
          right, ".", dplyr::sql_escape_ident(con, by$y)),
        collapse = " AND ",
        parens = TRUE)
    from =
      dplyr::build_sql(
        "SELECT * FROM ",
        dplyr::sql_subquery(con, x$query$sql, "_LEFT"), "\n",
        "LEFT SEMI JOIN ",
        dplyr::sql_subquery(con, y$query$sql, "_RIGHT"), "\n",
        "  ON ", on)
    attr(from, "vars") = x$select
    from}
