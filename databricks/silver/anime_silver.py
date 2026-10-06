from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    expr,
    lit,
    lower,
    regexp_replace,
    to_date,
    trim,
    when,
)


dbutils.widgets.text(
    "ingestion_date",
    "2026-10-05",
)

ingestion_date = dbutils.widgets.get(
    "ingestion_date"
)


def merge_to_silver(
    source_df,
    table_name,
    merge_condition,
):
    if spark.catalog.tableExists(table_name):

        target = DeltaTable.forName(
            spark,
            table_name,
        )

        (
            target.alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )


# ============================================================
# TENRAI
# ============================================================

tenrai_bronze = (
    spark.table(
        "anime_project.bronze.tenrai_anime_raw"
    )
    .filter(
        col("ingestion_date")
        == to_date(lit(ingestion_date))
    )
)


tenrai_silver = (
    tenrai_bronze

    .select(
        col("anime.mal_id")
        .cast("long")
        .alias("mal_id"),

        coalesce(
            col("anime.title_english"),
            col("anime.title"),
        ).alias("title"),

        col("anime.title_japanese")
        .alias("native_title"),

        col("anime.type")
        .alias("format"),

        col("anime.episodes")
        .cast("int")
        .alias("episodes"),

        col("anime.status")
        .alias("status"),

        col("anime.score")
        .cast("double")
        .alias("score_10"),

        col("anime.popularity")
        .cast("int")
        .alias("popularity"),

        col("anime.members")
        .cast("long")
        .alias("members"),

        col("anime.favorites")
        .cast("long")
        .alias("favorites"),

        expr(
            "transform(anime.genres, x -> x.name)"
        ).alias("genres"),

        col("anime.synopsis")
        .alias("description"),

        col("anime.year")
        .cast("int")
        .alias("year"),

        col("ingestion_date"),
    )

    .filter(
        col("mal_id").isNotNull()
    )

    .dropDuplicates(
        ["mal_id"]
    )

    .withColumn(
        "title",
        trim(col("title"))
    )

    .withColumn(
        "native_title",
        trim(col("native_title"))
    )

    .withColumn(
        "description",
        trim(
            regexp_replace(
                col("description"),
                "<[^>]+>",
                ""
            )
        )
    )

    .withColumn(
        "format",
        lower(
            trim(col("format"))
        )
    )

    .withColumn(
        "status",
        lower(
            trim(col("status"))
        )
    )

    .withColumn(
        "is_airing",
        when(
            col("status").isin(
                "currently airing",
                "airing",
            ),
            True
        ).otherwise(False)
    )

    .withColumn(
        "episode_category",
        when(
            col("episodes").isNull(),
            "unknown"
        )
        .when(
            col("episodes") <= 12,
            "short"
        )
        .when(
            col("episodes") <= 26,
            "medium"
        )
        .otherwise(
            "long"
        )
    )

    .withColumn(
        "release_decade",
        when(
            col("year").isNotNull(),
            (
                (col("year") / 10)
                .cast("int") * 10
            )
        )
    )

    .withColumn(
        "_updated_at",
        current_timestamp()
    )
)


merge_to_silver(
    tenrai_silver,
    "anime_project.silver.tenrai_anime",
    "target.mal_id = source.mal_id",
)


# ============================================================
# ANILIST
# ============================================================

anilist_bronze = (
    spark.table(
        "anime_project.bronze.anilist_anime_raw"
    )
    .filter(
        col("ingestion_date")
        == to_date(lit(ingestion_date))
    )
)


anilist_silver = (
    anilist_bronze

    .select(
        col("anime.id")
        .cast("long")
        .alias("anilist_id"),

        col("anime.idMal")
        .cast("long")
        .alias("mal_id"),

        coalesce(
            col("anime.title.english"),
            col("anime.title.romaji"),
        ).alias("title"),

        col("anime.title.native")
        .alias("native_title"),

        col("anime.format")
        .alias("format"),

        col("anime.episodes")
        .cast("int")
        .alias("episodes"),

        col("anime.status")
        .alias("status"),

        col("anime.averageScore")
        .cast("double")
        .alias("score_100"),

        col("anime.popularity")
        .cast("long")
        .alias("popularity"),

        col("anime.genres")
        .alias("genres"),

        col("anime.description")
        .alias("description"),

        col("anime.season")
        .alias("season"),

        col("anime.seasonYear")
        .cast("int")
        .alias("year"),

        col("ingestion_date"),
    )

    .filter(
        col("anilist_id").isNotNull()
    )

    .dropDuplicates(
        ["anilist_id"]
    )

    .withColumn(
        "title",
        trim(col("title"))
    )

    .withColumn(
        "native_title",
        trim(col("native_title"))
    )

    .withColumn(
        "description",
        trim(
            regexp_replace(
                col("description"),
                "<[^>]+>",
                ""
            )
        )
    )

    .withColumn(
        "format",
        lower(
            trim(col("format"))
        )
    )

    .withColumn(
        "status",
        lower(
            trim(col("status"))
        )
    )

    .withColumn(
        "score_10",
        col("score_100") / 10
    )

    .withColumn(
        "is_airing",
        when(
            col("status") == "releasing",
            True
        ).otherwise(False)
    )

    .withColumn(
        "episode_category",
        when(
            col("episodes").isNull(),
            "unknown"
        )
        .when(
            col("episodes") <= 12,
            "short"
        )
        .when(
            col("episodes") <= 26,
            "medium"
        )
        .otherwise(
            "long"
        )
    )

    .withColumn(
        "release_decade",
        when(
            col("year").isNotNull(),
            (
                (col("year") / 10)
                .cast("int") * 10
            )
        )
    )

    .withColumn(
        "_updated_at",
        current_timestamp()
    )
)


merge_to_silver(
    anilist_silver,
    "anime_project.silver.anilist_anime",
    "target.anilist_id = source.anilist_id",
)


# ============================================================
# TMDB
# ============================================================

tmdb_bronze = (
    spark.table(
        "anime_project.bronze.tmdb_anime_raw"
    )
    .filter(
        col("ingestion_date")
        == to_date(lit(ingestion_date))
    )
)


tmdb_silver = (
    tmdb_bronze

    .select(
        col("anime.id")
        .cast("long")
        .alias("tmdb_id"),

        col("anime._tmdb_media_type")
        .alias("media_type"),

        coalesce(
            col("anime.name"),
            col("anime.title"),
        ).alias("title"),

        coalesce(
            col("anime.original_name"),
            col("anime.original_title"),
        ).alias("original_title"),

        col("anime.overview")
        .alias("description"),

        col("anime.vote_average")
        .cast("double")
        .alias("score_10"),

        col("anime.vote_count")
        .cast("long")
        .alias("vote_count"),

        col("anime.popularity")
        .cast("double")
        .alias("popularity"),

        col("anime.genre_ids")
        .alias("genre_ids"),

        coalesce(
            col("anime.first_air_date"),
            col("anime.release_date"),
        ).alias("release_date"),

        col("ingestion_date"),
    )

    .filter(
        col("tmdb_id").isNotNull()
        & col("media_type").isNotNull()
    )

    .dropDuplicates(
        [
            "tmdb_id",
            "media_type",
        ]
    )

    .withColumn(
        "title",
        trim(col("title"))
    )

    .withColumn(
        "original_title",
        trim(col("original_title"))
    )

    .withColumn(
        "description",
        trim(col("description"))
    )

    .withColumn(
        "media_type",
        lower(
            trim(col("media_type"))
        )
    )

    .withColumn(
        "release_date",
        to_date(
            col("release_date")
        )
    )

    .withColumn(
        "year",
        expr(
            "try_cast(year(release_date) AS INT)"
        )
    )

    .withColumn(
        "release_decade",
        when(
            col("year").isNotNull(),
            (
                (col("year") / 10)
                .cast("int") * 10
            )
        )
    )

    .withColumn(
        "_updated_at",
        current_timestamp()
    )
)


merge_to_silver(
    tmdb_silver,
    "anime_project.silver.tmdb_anime",
    """
    target.tmdb_id = source.tmdb_id
    AND target.media_type = source.media_type
    """,
)


# ============================================================
# IMDB TITLE BASICS
# ============================================================

imdb_basics = (
    spark.table(
        "anime_project.bronze.imdb_title_basics_raw"
    )
    .filter(
        col("ingestion_date")
        == to_date(lit(ingestion_date))
    )
)


imdb_basics_silver = (
    imdb_basics

    .select(
        col("tconst")
        .alias("imdb_id"),

        col("titleType")
        .alias("title_type"),

        col("primaryTitle")
        .alias("title"),

        col("originalTitle")
        .alias("original_title"),

        expr(
            "try_cast(startYear AS INT)"
        ).alias("year"),

        expr(
            "try_cast(runtimeMinutes AS INT)"
        ).alias("runtime_minutes"),

        col("genres"),

        col("ingestion_date"),
    )

    .filter(
        col("imdb_id").isNotNull()
    )

    .dropDuplicates(
        ["imdb_id"]
    )

    .withColumn(
        "title",
        trim(col("title"))
    )

    .withColumn(
        "original_title",
        trim(col("original_title"))
    )

    .withColumn(
        "title_type",
        lower(
            trim(col("title_type"))
        )
    )

    .withColumn(
        "release_decade",
        when(
            col("year").isNotNull(),
            (
                (col("year") / 10)
                .cast("int") * 10
            )
        )
    )

    .withColumn(
        "_updated_at",
        current_timestamp()
    )
)


merge_to_silver(
    imdb_basics_silver,
    "anime_project.silver.imdb_titles",
    "target.imdb_id = source.imdb_id",
)


# ============================================================
# IMDB RATINGS
# ============================================================

imdb_ratings = (
    spark.table(
        "anime_project.bronze.imdb_title_ratings_raw"
    )
    .filter(
        col("ingestion_date")
        == to_date(lit(ingestion_date))
    )
)


imdb_ratings_silver = (
    imdb_ratings

    .select(
        col("tconst")
        .alias("imdb_id"),

        expr(
            "try_cast(averageRating AS DOUBLE)"
        ).alias("score_10"),

        expr(
            "try_cast(numVotes AS BIGINT)"
        ).alias("vote_count"),

        col("ingestion_date"),
    )

    .filter(
        col("imdb_id").isNotNull()
    )

    .dropDuplicates(
        ["imdb_id"]
    )

    .withColumn(
        "_updated_at",
        current_timestamp()
    )
)


merge_to_silver(
    imdb_ratings_silver,
    "anime_project.silver.imdb_ratings",
    "target.imdb_id = source.imdb_id",
)


# ============================================================
# ANIME MASTER
# ============================================================

tenrai = spark.table(
    "anime_project.silver.tenrai_anime"
)

anilist = spark.table(
    "anime_project.silver.anilist_anime"
)


anime_master = (
    tenrai.alias("t")

    .join(
        anilist.alias("a"),
        col("t.mal_id")
        == col("a.mal_id"),
        "full_outer",
    )

    .select(
        coalesce(
            col("t.mal_id"),
            col("a.mal_id"),
        ).alias("mal_id"),

        col("a.anilist_id"),

        coalesce(
            col("t.title"),
            col("a.title"),
        ).alias("title"),

        coalesce(
            col("t.native_title"),
            col("a.native_title"),
        ).alias("native_title"),

        coalesce(
            col("t.format"),
            col("a.format"),
        ).alias("format"),

        coalesce(
            col("t.episodes"),
            col("a.episodes"),
        ).alias("episodes"),

        coalesce(
            col("t.status"),
            col("a.status"),
        ).alias("status"),

        coalesce(
            col("t.is_airing"),
            col("a.is_airing"),
            lit(False),
        ).alias("is_airing"),

        coalesce(
            col("t.year"),
            col("a.year"),
        ).alias("year"),

        coalesce(
            col("t.release_decade"),
            col("a.release_decade"),
        ).alias("release_decade"),

        coalesce(
            col("t.episode_category"),
            col("a.episode_category"),
        ).alias("episode_category"),

        coalesce(
            col("t.genres"),
            col("a.genres"),
        ).alias("genres"),

        col("t.score_10")
        .alias("mal_score"),

        col("a.score_10")
        .alias("anilist_score"),

        coalesce(
            col("t.popularity"),
            col("a.popularity"),
        ).alias("popularity"),

        coalesce(
            col("t.description"),
            col("a.description"),
        ).alias("description"),

        current_timestamp()
        .alias("_updated_at"),
    )

    .filter(
        col("title").isNotNull()
    )
)


(
    anime_master.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "anime_project.silver.anime_master"
    )
)