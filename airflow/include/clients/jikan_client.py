import logging
import requests

from include.validation.anime_validation import validate_jikan_response

JIKAN_URL = "https://api.jikan.moe/v4/top/anime"


def fetch_jikan_anime():
    logging.info("Requesting Jikan API")

    response = requests.get(
        JIKAN_URL,
        params={"page": 1},
        timeout=30,
    )

    response.raise_for_status()

    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError("Jikan returned invalid JSON") from exc

    validate_jikan_response(data)

    anime = data["data"]

    logging.info("Jikan returned %s records", len(anime))

    return {
        "source": "jikan",
        "record_count": len(anime),
        "data": anime,
    }
