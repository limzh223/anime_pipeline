import logging
import requests

from include.validation.anime_validation import validate_tenrai_response


TENRAI_URL = "https://api.tenrai.org/v1/top/anime"


def fetch_tenrai_anime():
    logging.info("Requesting Tenrai API")

    response = requests.get(
        TENRAI_URL,
        params={"page": 1},
        timeout=30,
    )

    response.raise_for_status()

    try:
        data = response.json()

    except ValueError as exc:
        raise ValueError(
            "Tenrai returned invalid JSON"
        ) from exc

    validate_tenrai_response(data)

    anime = data["data"]

    logging.info(
        "Tenrai returned %s records",
        len(anime),
    )

    return {
        "source": "tenrai",
        "record_count": len(anime),
        "data": anime,
    }