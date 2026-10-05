import logging
import requests

from include.validation.anime_validation import validate_anilist_response

ANILIST_URL = "https://graphql.anilist.co"

ANILIST_QUERY = '''
query {
  Page(page: 1, perPage: 50) {
    media(type: ANIME) {
      id
      idMal
      title {
        romaji
        english
        native
      }
      episodes
      averageScore
      popularity
      genres
      description
      season
      seasonYear
      format
      status
    }
  }
}
'''


def fetch_anilist_anime():
    logging.info("Requesting AniList GraphQL API")

    response = requests.post(
        ANILIST_URL,
        json={"query": ANILIST_QUERY},
        timeout=30,
    )

    response.raise_for_status()

    try:
        data = response.json()
    except ValueError as exc:
        raise ValueError("AniList returned invalid JSON") from exc

    validate_anilist_response(data)

    media = data["data"]["Page"]["media"]

    logging.info("AniList returned %s records", len(media))

    return {
        "source": "anilist",
        "record_count": len(media),
        "data": media,
    }
