

## 0.6.0

- New java extensions capability with `add_jar`, `add_function` and `add_extension`
- Additional params can be passed at src creation, courtesy @MarcinKosinski
- `cross_join` added
- Many small changes and bug fixes
  - fix #7
  - CSV reader fails early by default rather than skipping bad rows
  - Preparations for null values support in CSV, pending spark updates
  - make mode argument to CSV reader explicit
  - drop NAs in tests until supported in spark
  - switch to rzilla org and achive.rzilla.org package archive
  - workaround for hadley/dplyr#1651
  - more dealing with backend case insensitivity
  - fix print info for Hive src
  - some intial support for backend types such as maps and structs (not available to user yet,  but ask if you need it)

 
## Previous releases


See https://github.com/piccolbo/dplyr.spark.hive/releases

