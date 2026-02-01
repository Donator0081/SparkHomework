from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder \
        .appName("Check Boston Crimes Mart") \
        .getOrCreate()

    df = spark.read.parquet("output")

    print("=== Sample data ===")
    df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
