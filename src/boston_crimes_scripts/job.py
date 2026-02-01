import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, percentile_approx, split, concat_ws,
    collect_list, row_number, desc, date_format, regexp_replace
)
from pyspark.sql.window import Window


def main(input_path, output_path):
    spark = SparkSession.builder.appName("Boston Crime Aggregation").getOrCreate()

    crimes_df = spark.read.csv(os.path.join(input_path, "crime.csv"), header=True, inferSchema=True)
    codes_df = spark.read.csv(os.path.join(input_path, "offense_codes.csv"), header=True, inferSchema=True)

    crimes_df = crimes_df.dropDuplicates()
    codes_df = codes_df.dropDuplicates(["CODE"])

    codes_df = codes_df.withColumn("crime_type_full", split(col("NAME"), "-").getItem(0))
    crimes_df = crimes_df.join(codes_df.select("CODE", "crime_type_full"), crimes_df.OFFENSE_CODE == codes_df.CODE,
                               "left")

    crimes_df = crimes_df.withColumn("year_month", date_format("OCCURRED_ON_DATE", "yyyy-MM"))

    type_counts = crimes_df.groupBy("district", "crime_type_full").count()
    freq_window = Window.partitionBy("district").orderBy(desc("count"))
    type_counts = type_counts.withColumn("rank", row_number().over(freq_window))

    top3_types = type_counts.filter(col("rank") <= 3) \
        .groupBy("district") \
        .agg(concat_ws(",", collect_list("crime_type_full")).alias("frequent_crime_types"))

    top3_types = top3_types.withColumn(
        "frequent_crime_types",
        regexp_replace(col("frequent_crime_types"), r"\s*,\s*", ", ")
    )

    main_type = type_counts.filter(col("rank") == 1) \
        .select("district", col("crime_type_full").alias("crime_type"))

    monthly_counts = crimes_df.groupBy("district", "year_month").agg(count("*").alias("monthly_count"))
    monthly_median = monthly_counts.groupBy("district") \
        .agg(percentile_approx("monthly_count", 0.5).alias("crimes_monthly"))

    metrics = crimes_df.groupBy("district").agg(
        count("*").alias("crimes_total"),
        avg("LAT").alias("lat"),
        avg("LONG").alias("lng")
    )

    result = metrics.join(monthly_median, on="district", how="left") \
        .join(top3_types, on="district", how="left") \
        .join(main_type, on="district", how="left")

    result = result.dropna(subset=["district", "crimes_total", "crimes_monthly",
                                   "frequent_crime_types", "crime_type", "lat", "lng"])

    result = result.select("district", "crimes_total", "crimes_monthly",
                           "frequent_crime_types", "crime_type", "lat", "lng")

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    output_path_clean = os.path.abspath(output_path).replace("\\", "/")

    result.coalesce(1).write.mode("overwrite").parquet(f"file:///{output_path_clean}")

    spark.stop()
    print(f"Parquet сохранен в {output_path_clean}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: spark-submit job.py <path_to_data> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
