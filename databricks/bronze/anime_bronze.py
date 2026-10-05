from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
)


JIKAN_PATH = (
    "/Volumes/anime_project/bronze/raw/jikan/"
)

ANILIST_PATH = (
    "/Volumes/anime_project/bronze/raw/anilist/"
)

TMDB_PATH = (
    "/Volumes/anime_project/bronze/raw/tmdb/"
)

IMDB_BASICS_PATH = (
    "/Volumes/anime_project/bronze/raw/imdb/"
    "title_basics.tsv.gz"
)

IMDB_RATINGS_PATH = (
    "/Volumes/anime_project/bronze/raw/imdb/"
    "title_ratings.tsv.gz"
)


def load_json_source_to_bronze(
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


def load_imdb_to_bronze(
    source_path,
    table_name,
):
    bronze_df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .csv(source_path)
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


load_json_source_to_bronze(
    JIKAN_PATH,
    "anime_project.bronze.jikan_anime_raw",
)

load_json_source_to_bronze(
    ANILIST_PATH,
    "anime_project.bronze.anilist_anime_raw",
)

load_json_source_to_bronze(
    TMDB_PATH,
    "anime_project.bronze.tmdb_anime_raw",
)

load_imdb_to_bronze(
    IMDB_BASICS_PATH,
    "anime_project.bronze.imdb_title_basics_raw",
)

load_imdb_to_bronze(
    IMDB_RATINGS_PATH,
    "anime_project.bronze.imdb_title_ratings_raw",
)