from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    round,
    array_join,
    concat_ws,
    when,
    current_timestamp,
)


anime = spark.table(
    "anime_project.silver.anime_master"
)


anime_gold = (
    anime

    .filter(
        col("title").isNotNull()
    )

    .withColumn(
        "combined_score",
        round(
            when(
                col("mal_score").isNotNull()
                & col("anilist_score").isNotNull(),

                (
                    col("mal_score")
                    + col("anilist_score")
                ) / 2
            )

            .when(
                col("mal_score").isNotNull(),
                col("mal_score")
            )

            .otherwise(
                col("anilist_score")
            ),

            2,
        )
    )

    .withColumn(
        "genre_text",
        array_join(
            col("genres"),
            ", "
        )
    )

    .withColumn(
        "recommendation_tier",
        when(
            col("combined_score") >= 9,
            "excellent"
        )
        .when(
            col("combined_score") >= 8,
            "highly_recommended"
        )
        .when(
            col("combined_score") >= 7,
            "recommended"
        )
        .otherwise(
            "general"
        )
    )

    .withColumn(
        "llm_context",
        concat_ws(
            " | ",

            col("title"),

            concat_ws(
                "",
                lit("Genres: "),
                col("genre_text"),
            ),

            concat_ws(
                "",
                lit("Format: "),
                col("format"),
            ),

            concat_ws(
                "",
                lit("Episodes: "),
                col("episodes")
                .cast("string"),
            ),

            concat_ws(
                "",
                lit("Status: "),
                col("status"),
            ),

            concat_ws(
                "",
                lit("Year: "),
                col("year")
                .cast("string"),
            ),

            concat_ws(
                "",
                lit("Score: "),
                col("combined_score")
                .cast("string"),
            ),

            col("description"),
        )
    )

    .withColumn(
        "_generated_at",
        current_timestamp()
    )

    .select(
        "mal_id",
        "anilist_id",
        "title",
        "title_japanese",
        "title_native",
        "format",
        "episodes",
        "episode_category",
        "status",
        "is_airing",
        "year",
        "release_decade",
        "genres",
        "genre_text",
        "mal_score",
        "anilist_score",
        "combined_score",
        "recommendation_tier",
        "description",
        "llm_context",
        "_generated_at",
    )
)


(
    anime_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "anime_project.gold.anime_recommendations"
    )
)