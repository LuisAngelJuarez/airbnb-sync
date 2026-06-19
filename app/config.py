import os
import json
from typing import List, Dict, Any

TIMEZONE = "America/Mexico_City"

LISTINGS_ENV_VAR = "LISTINGS_JSON"

REDIS_URL = os.getenv("REDIS_URL")


def load_listings() -> List[Dict[str, Any]]:
    """
    Lee la lista de cabañas desde la variable de entorno LISTINGS_JSON.

    Espera algo así:

    [
      {
        "name": "Cabaña 2 personas",
        "airbnb_ical_url": "https://www.airbnb.com/calendar/ical/XXX.ics",
        "tidycal_booking_type_id": 1234567,
        "airbnb_contact_email": "airbnb-cabana@tudominio.com",
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

    required = {"name", "airbnb_ical_url", "init_time", "finish_time"}
    for i, listing in enumerate(listings):
        missing = required - set(listing.keys())
        if missing:
            raise RuntimeError(f"Listing #{i} le faltan campos: {', '.join(sorted(missing))}")

    return listings
