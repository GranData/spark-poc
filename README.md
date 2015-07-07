# spark-poc

several use-cases for playing around with Apache Spark.


## Trying Spark DataFrames
In this project there is a mini Proof of concept using Apache Spark Dataframes.

Contains to tests:
1) JoinUsingPlainRdd, a join of a table (file gz of about 10mb) with itself, using plain RDDs.
2) JoinUsingDataFramesMain, the same join, but using DataFrames instead of RDDs.
 -This uses spark-csv (a thirty party library) for import the file with its schema.


Observations:
Join using Dataframes runs in less than half time the join using RDDs.


Come and see...
