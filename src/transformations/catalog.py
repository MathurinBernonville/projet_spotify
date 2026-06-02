"""Fonctions de transformation du catalogue musical."""


def normalize_artist_name(name: str) -> str:
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list:
    errors = []
    required = ["id", "artist_id", "title", "duration_ms"]
    for field in required:
        if field not in track:
            errors.append(f"Champ manquant : {field}")
    if "duration_ms" in track:
        if track["duration_ms"] <= 0:
            errors.append("duration_ms doit être positif")
        if track["duration_ms"] > 3_600_000:
            errors.append("duration_ms trop long (> 1h)")
    return errors


def deduplicate_artists(artists: list) -> list:
    seen = set()
    result = []
    for artist in artists:
        key = (normalize_artist_name(artist.get("name", "")), artist.get("label", ""))
        if key not in seen:
            seen.add(key)
            result.append({**artist, "name": normalize_artist_name(artist.get("name"))})
    return result
