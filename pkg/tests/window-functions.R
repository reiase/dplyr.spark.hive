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

copy_to_from_local = dplyr.spark.hive:::copy_to_from_local

my_db = src_SparkSQL()

library(Lahman)
batting = {
  if(db_has_table(my_db$con, "batting"))
    batting = tbl(my_db, "batting")
  else
    copy_to_from_local(my_db, Batting, "batting")}
batting = select(batting, playerID, yearID, teamID, G, AB:H)
batting = arrange(batting, playerID, yearID, teamID)
players = group_by(batting, playerID)
cache(batting)
cache(players)
# For each player, find the two years with most hits
filter(players, min_rank(desc(H)) <= 2 & H > 0)
# Within each player, rank each year by the number of games played
mutate(players, g_rank = min_rank(G))

# For each player, find every year that was better than the previous year
filter(players, G > lag(G))
# For each player, compute avg change in games played per year
mutate(players, g_change = (G - lag(G)) / (yearID - lag(yearID)))

# For each player, find all where they played more games than average
filter(players, G > mean(G))
# For each, player compute a z score based on number of games played
mutate(players, g_z = (G - mean(G)) / sd(G))

