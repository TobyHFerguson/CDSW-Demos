# A minimal set of commands to check that Hail is working correctly.

from pyspark.sql import SparkSession
spark = SparkSession\
    .builder\
    .config("spark.jars", "/home/sense/hail-all-spark.jar")\
    .config("spark.submit.pyFiles", "/home/sense/hail-python.zip")\
    .config("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,is.hail.io.compress.BGzipCodec,org.apache.hadoop.io.compress.GzipCodec")\
    .config("spark.sql.files.openCostInBytes", "1099511627776")\
    .config("spark.sql.files.maxPartitionBytes", "1099511627776")\
    .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "1099511627776")\
    .config("spark.hadoop.parquet.block.size", "1099511627776")\
    .appName("Hail")\
    .getOrCreate()
sc = spark.sparkContext
import sys
sys.path.append('/home/sense/hail-python.zip')

import hail
hc = hail.HailContext(sc)

hc.import_vcf('sample.vcf').write('sample.vds', overwrite=True)

# Have a look at the files that were created:
!hadoop fs -ls -R sample.vds

# Stop the Spark application to free up resources on the cluster:
spark.stop()