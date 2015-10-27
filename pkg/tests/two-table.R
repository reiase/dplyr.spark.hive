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

# derivative of dplyr introductory material, http://github.com/hadley/dplyr
# presumably under MIT licenselibrary(dplyr)

library(dplyr)
library(dplyr.spark.hive)
library(purrr)

copy_to_from_local = dplyr.spark.hive:::copy_to_from_local

my_db = src_SparkSQL()

library(nycflights13)

cond.copy =
  function(src, data, name) {
    if(db_has_table(src$con, name))
      tbl(src, name)
    else
      copy_to_from_local(src, data, name)}

flights = cond.copy(my_db, flights, "flights")
airlines = cond.copy(my_db, airlines, "airlines")
weather = cond.copy(my_db, weather, "weather")
planes = cond.copy(my_db, planes, "planes")
airports = cond.copy(my_db, airports, "airports")

flights2 =
  flights %>%
  select(year:day, hour, origin, dest, tailnum, carrier)

left_join(flights2, airlines)

left_join(flights2, weather)

left_join(flights2, planes, by = "tailnum")

left_join(flights2, airports, c("dest" = "faa"))
# not in dplyr dplyr/#1181
# flights2 %>% left_join(airports, dest == faa)

left_join(flights2, airports, c("origin" = "faa"))

(df1 = data_frame(x = c(1, 2), y = 2:1))
(df2 = data_frame(x = c(1, 3), a = 10, b = "a"))

{if(!db_has_table(my_db$con, "df1")) {
  copy_to_from_local(my_db, df1, "df1")
  copy_to_from_local(my_db, df2, "df2")}
else{
  df1 = tbl(my_db, "df1")
  df2 = tbl(my_db, "df2")}}

df1
df2

inner_join(df1, df2) %>% collect

left_join(df1, df2) %>% collect

right_join(df1, df2) %>% collect

left_join(df2, df2) %>% collect

full_join(df2, df1) %>% collect

# not implemented yet
# flights %>%
#   anti_join(planes, by = "tailnum") %>%
#   count(tailnum, sort = TRUE)

inner_join(df1, df2, by = "x") %>% collect
semi_join(df1, df2, by = "x") %>% collect

#need better examples here
#intersect(df1, df2)
#union(df1, df2)
#setdiff(df1, df2)
#setdiff(df2, df1)



