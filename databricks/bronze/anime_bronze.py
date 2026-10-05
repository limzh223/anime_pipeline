from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
)


TENRAI_PATH = (
    "/Volumes/anime_project/bronze/raw/tenrai/"
)

ANILIST_PATH = (
    "/Volumes/anime_project/bronze/raw/anilist/"
)

TMDB_PATH = (
    "/Volumes/anime_project/bronze/raw/tmdb/"
)


def load_json_to_bronze(
    source_path,
    table_name,
):

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
        .withColumn(
            "_ingested_at",
            current_timestamp(),
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )


load_json_to_bronze(
    TENRAI_PATH,
    "anime_project.bronze.tenrai_anime_raw",
)

load_json_to_bronze(
    ANILIST_PATH,
    "anime_project.bronze.anilist_anime_raw",
)

load_json_to_bronze(
    TMDB_PATH,
    "anime_project.bronze.tmdb_anime_raw",
)