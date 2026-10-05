from pyspark.sql.functions import col, current_timestamp, explode

JIKAN_PATH = "s3://YOUR_BUCKET/raw/jikan/"
ANILIST_PATH = "s3://YOUR_BUCKET/raw/anilist/"


def load_source_to_bronze(source_path, table_name):
    raw_df = (
        spark.read
        .option("multiLine", True)
        .json(source_path)
    )

    bronze_df = (
        raw_df
        .select(
            col("source"),
            col("ingestion_date"),
            explode("data").alias("anime"),
        )
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )


load_source_to_bronze(
    JIKAN_PATH,
    "anime_project.bronze.jikan_anime_raw",
)

load_source_to_bronze(
    ANILIST_PATH,
    "anime_project.bronze.anilist_anime_raw",
)
