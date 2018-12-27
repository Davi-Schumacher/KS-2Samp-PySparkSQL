# KS-2Samp-PySparkSQL
Two-sample Kolmogorov-Smirnov test implemented in PySpark SQL

I wrote this because I needed a KS implementation that operated directly on a PySpark SQL dataframe. A Spark built-in two-sample KS test is only available through the RDD API, but the conversion to an RDD was slowing me down too much.

The KS test is only valid for continuous distributions.
