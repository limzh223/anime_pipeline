import logging
import os

import requests


TMDB_BASE_URL = "https://api.themoviedb.org/3"


def fetch_tmdb_anime():

    token = os.getenv("TMDB_READ_ACCESS_TOKEN")

    if not token:
        raise ValueError(
            "TMDB_READ_ACCESS_TOKEN environment variable is not set"
        )

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
    }

    # TMDB genre 16 = Animation
    # Japanese original-language titles are used here as
    # an anime-oriented filter.
    common_params = {
        "with_genres": "16",
        "with_original_language": "ja",
        "include_adult": "false",
        "language": "en-US",
        "page": 1,
    }

    logging.info("Requesting TMDB anime TV data")

    tv_response = requests.get(
        f"{TMDB_BASE_URL}/discover/tv",
        headers=headers,
        params=common_params,
        timeout=30,
    )

    tv_response.raise_for_status()

    logging.info("Requesting TMDB anime movie data")

    movie_response = requests.get(
        f"{TMDB_BASE_URL}/discover/movie",
        headers=headers,
        params=common_params,
        timeout=30,
    )

    movie_response.raise_for_status()

    try:
        tv_data = tv_response.json()
        movie_data = movie_response.json()

    except ValueError as exc:
        raise ValueError(
            "TMDB returned invalid JSON"
        ) from exc

    tv_results = tv_data.get("results", [])
    movie_results = movie_data.get("results", [])

    if not tv_results and not movie_results:
        raise ValueError(
            "TMDB returned zero anime records"
        )

    # Add source type so Silver can distinguish them later.
    for record in tv_results:
        record["_tmdb_media_type"] = "tv"

    for record in movie_results:
        record["_tmdb_media_type"] = "movie"

    records = tv_results + movie_results

    logging.info(
        "TMDB returned %s records",
        len(records),
    )

    return {
        "source": "tmdb",
        "record_count": len(records),
        "data": records,
    }