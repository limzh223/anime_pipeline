def validate_tenrai_response(data):
    if not data:
        raise ValueError(
            "Tenrai returned empty JSON"
        )

    if "data" not in data:
        raise ValueError(
            "Tenrai response missing 'data'"
        )

    if not data["data"]:
        raise ValueError(
            "Tenrai returned zero anime records"
        )


def validate_anilist_response(data):
    if not data:
        raise ValueError(
            "AniList returned empty JSON"
        )

    if "errors" in data:
        raise ValueError(
            f"AniList GraphQL error: {data['errors']}"
        )

    if "data" not in data:
        raise ValueError(
            "AniList response missing 'data'"
        )

    page = data["data"].get("Page")

    if not page:
        raise ValueError(
            "AniList response missing Page"
        )

    media = page.get("media")

    if not media:
        raise ValueError(
            "AniList returned zero anime records"
        )