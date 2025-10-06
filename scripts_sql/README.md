### Those scripts are used to create tables in silver & gold tables
There're only one script needed for Silver layer as it denormalizes all Bronze tables into one single table. It leverages the ARRAY structure of BigQuery which reduces data scanning jobs in the remaining Gold scripts during JOINs processing in Star Schema, which will scale up significantly when the dataset reaches GBs of weight.
For performance improvement of reducing billing cost for data scanning in create gold tables jobs, please visit benchmark_performance_test in test for further details.
