## What is this?

PySpark Jobs for investigating prevalence of ML Opt–Out Protocols, written by [Alex Xue](https://commoncrawl.org/team/alex-xue) as part of the blog post [A Further Look Into the Prevalence of Various ML Opt–Out Protocols](https://commoncrawl.org/blog/a-further-look-into-the-prevalence-of-various-ml-opt-out-protocols).

### How Do I Run It?

Requires `sparkcc.py` from [commoncrawl/cc-pyspark](https://github.com/commoncrawl/cc-pyspark/blob/main/sparkcc.py).

Setup is the same as [cc-pyspark](https://github.com/commoncrawl/cc-pyspark). Make sure you have an `./input` directory.

To run the jobs:

```
$SPARK_HOME/bin/spark-submit job_name.py \
    --num_output_partitions 1 --log_level WARN \
    ./input/test_warc.txt output_file_name
```

and specifically to run html_metatag_count.py (which has a different output schema)

```
$SPARK_HOME/bin/spark-submit ./html_metatag_count.py \
    --num_output_partitions 1 --log_level WARN --tuple_key_schema True \
    ./input/test_warc.txt output_file_name
```
