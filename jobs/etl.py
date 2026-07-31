from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--source")
parser.add_argument("--destination")
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true").csv(args.source)

df.writeTo(args.destination).createOrReplace()

spark.stop()