import datetime as dt
from typing import Dict, Any, List, Set

from zoneinfo import ZoneInfo

from ..config import TIMEZONE
from ..connectors.tidycal_api import tidycal_list_bookings_in_range, booking_date_from_starts_at_utc

TZ_LOCAL = ZoneInfo(TIMEZONE)


def get_blocked_nights_for_listing(
    listing_cfg: Dict[str, Any],
    days_ahead: int = 365,
) -> List[str]:
    """
    Devuelve una lista de noches bloqueadas (YYYY-MM-DD) PARA EL FUTURO
    para la cabaña indicada en listing_cfg.
    """

    booking_type_id = listing_cfg["tidycal_booking_type_id"]

    # 👇 Clave: solo miramos desde HOY hacia adelante
    today = dt.datetime.now(TZ_LOCAL).date()
    end_date = today + dt.timedelta(days=days_ahead)

    # Le pedimos a TidyCal solo bookings en [today, end_date)
    bookings = tidycal_list_bookings_in_range(start_date=today, end_date=end_date)

    blocked_dates: Set[dt.date] = set()

    for b in bookings:
        # Solo bookings de este tipo (esta cabaña)
        if b.get("booking_type_id") != booking_type_id:
            continue

        # (Por si acaso) ignorar canceladas
        if b.get("cancelled_at"):
            continue

        day = booking_date_from_starts_at_utc(b.get("starts_at"))
        if not day:
            continue

        # Extra filtro defensivo: por si TidyCal devolviera algo antes de hoy
        if day < today:
            continue

        blocked_dates.add(day)

    # Convertimos a lista ordenada de strings ISO
    return sorted(d.isoformat() for d in blocked_dates)


def build_availability_snapshot(
    listings: List[Dict[str, Any]],
    blocked_by_slug: Dict[str, Set[str]],
) -> Dict[str, Any]:
    """
    Construye el snapshot de disponibilidad para el bot, combinando:

      - info de listings.json (campo 'info')
      - noches bloqueadas calculadas (blocked_by_slug)

    No toca Redis, solo devuelve el dict listo para serializar a JSON.
    """
    now_local = dt.datetime.now(TZ_LOCAL).replace(microsecond=0)
    snapshot_listings: List[Dict[str, Any]] = []

    for l in listings:
        info = l.get("info", {})
        slug = info.get("slug")
        if not slug:
            # Fallback simple por si algún listing no trae slug
            slug = (
                l["name"]
                .lower()
                .replace(" ", "")
                .replace("á", "a")
                .replace("é", "e")
                .replace("í", "i")
                .replace("ó", "o")
                .replace("ú", "u")
                .replace("ñ", "n")
            )

        blocked_nights = sorted(blocked_by_slug.get(slug, set()))

        snapshot_listings.append(
            {
                "id": slug,
                "name": l["name"],
                "capacity": info.get("capacity"),
                "has_kitchen": info.get("has_kitchen"),
                "has_private_bathroom": info.get("has_private_bathroom"),
                "has_ac": info.get("has_ac"),
                "wifi": info.get("wifi"),
                "beds": info.get("beds", []),
                "description": info.get("description", ""),
                "tidycal_url": info.get("tidycal_url"),
                "blocked_nights": blocked_nights,
            }
        )

    snapshot = {
        "generated_at": now_local.isoformat(),
        "timezone": TIMEZONE,
        "listings": snapshot_listings,
    }

    print(f"[snapshot] Generado snapshot para bot con {len(snapshot_listings)} listings.")
    return snapshot
