from pyspark.sql.functions import (
    array_join,
    col,
    concat_ws,
    current_timestamp,
    lit,
    round,
    when,
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

            concat_ws(
                "",
                lit("Title: "),
                col("title"),
            ),

            concat_ws(
                "",
                lit("Native Title: "),
                col("native_title"),
            ),

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
                col("episodes").cast("string"),
            ),

            concat_ws(
                "",
                lit("Status: "),
                col("status"),
            ),

            concat_ws(
                "",
                lit("Year: "),
                col("year").cast("string"),
            ),

            concat_ws(
                "",
                lit("Score: "),
                col("combined_score").cast("string"),
            ),

            concat_ws(
                "",
                lit("Description: "),
                col("description"),
            ),
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
        "native_title",

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

        "popularity",

        "description",
        "llm_context",

        "_generated_at",
    )
)


(
    anime_gold.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "anime_project.gold.anime_recommendations"
    )
)