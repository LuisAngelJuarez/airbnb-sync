import os
import json
from typing import List, Dict, Any

# Zona horaria para los eventos en Google Calendar
TIMEZONE = "America/Mexico_City"

# Alcance necesario para la API de Calendar
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# Nombre de la variable de entorno que contiene la lista de cabañas
LISTINGS_ENV_VAR = "LISTINGS_JSON"

# Redis
REDIS_URL = os.getenv("REDIS_URL")
print(f"[config] REDIS_URL desde config.py = {repr(REDIS_URL)}")


def load_listings() -> List[Dict[str, Any]]:
    """
    Lee la lista de cabañas desde la variable de entorno LISTINGS_JSON.

    Espera algo así:

    [
      {
        "name": "Cabaña 2 personas",
        "airbnb_ical_url": "https://www.airbnb.com/calendar/ical/XXX.ics",
        "mirror_calendar_id": "xxx@group.calendar.google.com",
        "init_time": "14:00",
        "finish_time": "11:00"
      },
      ...
    ]
    """
    raw = os.environ.get(LISTINGS_ENV_VAR)
    if not raw:
        raise RuntimeError(f"Falta variable de entorno {LISTINGS_ENV_VAR}")

    listings = json.loads(raw)

    required = {"name", "airbnb_ical_url", "mirror_calendar_id", "init_time", "finish_time"}
    for i, listing in enumerate(listings):
        missing = required - set(listing.keys())
        if missing:
            raise RuntimeError(f"Listing #{i} le faltan campos: {', '.join(sorted(missing))}")

    return listings
