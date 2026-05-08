"""
Event logger for the dashboard.
All bot activity is logged here — signals, news, broker orders, confluences, insiders.
"""
import os
import json
from datetime import datetime, timezone

DIR        = os.path.dirname(os.path.abspath(__file__))
EVENTS_FILE = os.path.join(DIR, "bot_events.json")
MAX_EVENTS  = 500

TYPE_COLORS = {
    "NEWS":       "#f59e0b",
    "SIGNAL":     "#60a5fa",
    "CONFLUENCE": "#a78bfa",
    "BROKER":     "#34d399",
    "INSIDER":    "#fb923c",
    "CLOSE":      "#f87171",
    "HEARTBEAT":  "#6b7280",
    "ERROR":      "#ef4444",
    "INFO":       "#9ca3af",
}

def log_event(event_type: str, message: str, details: dict = None):
    events = _load()
    events.append({
        "ts":      datetime.now(timezone.utc).isoformat(),
        "type":    event_type.upper(),
        "msg":     message,
        "details": details or {},
        "color":   TYPE_COLORS.get(event_type.upper(), "#9ca3af"),
    })
    _save(events[-MAX_EVENTS:])

def get_events(n: int = 100) -> list:
    return _load()[-n:][::-1]  # newest first

def _load() -> list:
    try:
        with open(EVENTS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save(events: list):
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False)
