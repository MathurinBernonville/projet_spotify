"""Fonctions de validation des événements d'écoute."""

from datetime import datetime, timezone


def is_valid_listening_event(event: dict) -> bool:
    required = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
    for field in required:
        if field not in event or event[field] is None:
            return False

    # Timestamp dans le futur → suspect
    try:
        ts = event["timestamp"].replace("Z", "+00:00")
        event_time = datetime.fromisoformat(ts)
        if event_time.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc):
            return False
    except Exception:
        return False

    # Pattern bot : durée < 5s
    if event.get("duration_ms", 0) < 5000:
        return False

    return True
