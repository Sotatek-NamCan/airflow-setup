import argparse

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()

parser.add_argument("--source", required=True)
parser.add_argument("--destination", required=True)

args = parser.parse_args()


spark = (
    SparkSession.builder
    .appName("S3ToIceberg")
    .getOrCreate()
)

print(f"Reading from {args.source}")

df = spark.read.csv(args.source)

# Example transformation
df = df.dropDuplicates()

print(f"Writing to {args.destination}")

df.writeTo(args.destination).append()

spark.stop()