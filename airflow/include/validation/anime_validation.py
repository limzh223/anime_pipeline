def validate_jikan_response(data):
    if not data:
        raise ValueError("Jikan returned empty JSON")

    if "data" not in data:
        raise ValueError("Jikan response missing 'data'")

    if not data["data"]:
        raise ValueError("Jikan returned zero anime records")


def validate_anilist_response(data):
    if not data:
        raise ValueError("AniList returned empty JSON")

    if "errors" in data:
        raise ValueError(f"AniList GraphQL error: {data['errors']}")

    if "data" not in data:
        raise ValueError("AniList response missing 'data'")

    page = data["data"].get("Page")

    if not page:
        raise ValueError("AniList response missing Page")

    media = page.get("media")

    if not media:
        raise ValueError("AniList returned zero anime records")
