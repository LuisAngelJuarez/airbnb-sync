from typing import Dict, Any, List, Set

from .config import load_listings
from .services.airbnb_sync import fetch_airbnb_calendar, sync_airbnb_to_tidycal
from .services.availability_snapshot import get_blocked_nights_for_listing, build_availability_snapshot
from .connectors.redis_client import save_snapshot_to_redis


def sync_listing(listing_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flujo para UNA cabaña:
      1) Lee iCal de Airbnb y lo sincroniza con TidyCal (API) creando / cancelando bookings.
      2) Calcula noches bloqueadas desde TidyCal para el bot (snapshot Redis).
    """
    print("\n============================")
    print(f"Sincronizando: {listing_cfg['name']}")
    print("============================")

    airbnb_url = listing_cfg.get("airbnb_ical_url", "")
    airbnb_stats = {"created": 0, "cancelled": 0, "errors": 0}
    if airbnb_url:
        cal = fetch_airbnb_calendar(airbnb_url)
        airbnb_stats = sync_airbnb_to_tidycal(cal, listing_cfg)
    else:
        print(f"[sync_listing] {listing_cfg['name']}: sin airbnb_ical_url, se omite sync de Airbnb.")

    blocked_nights = get_blocked_nights_for_listing(listing_cfg)

    result_stats = {
        "created": airbnb_stats["created"],
        "updated": 0,
        "deleted": airbnb_stats["cancelled"],
        "errors": airbnb_stats["errors"],
    }

    print(f"[sync_listing] Final → {listing_cfg['name']}: {result_stats}\n")

    return {
        "stats": result_stats,
        "blocked_nights": blocked_nights,
    }


def sync_all() -> Dict[str, Dict[str, int]]:
    """
    Orquesta la sincronización para todas las cabañas definidas en listings.json
    y genera un snapshot de disponibilidad para el bot, guardándolo en Redis.
    """
    print("\n============================")
    print("   INICIANDO sync_all()")
    print("============================")

    listings = load_listings()

    print("[sync_all] Listings cargados:")
    for l in listings:
        print(
            f"  - {l['name']} → "
            f"airbnb_ical={l.get('airbnb_ical_url', 'N/A')} "
            f"booking_type_id={l.get('tidycal_booking_type_id', 'N/A')}"
        )

    result_stats: Dict[str, Dict[str, int]] = {}
    blocked_by_slug: Dict[str, Set[str]] = {}

    for listing in listings:
        name = listing["name"]
        info = listing.get("info", {})
        slug = info.get("slug") or (
            name
            .lower()
            .replace(" ", "")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
            .replace("ñ", "n")
        )

        sync_result = sync_listing(listing)
        result_stats[name] = sync_result["stats"]
        blocked_by_slug[slug] = sync_result["blocked_nights"]

    snapshot = build_availability_snapshot(listings, blocked_by_slug)

    try:
        save_snapshot_to_redis(snapshot)
    except Exception as e:
        print(f"[sync_all:⚠ error] No se pudo guardar snapshot en Redis: {e}")

    print("\n📌 RESULTADOS GLOBALES:")
    print(result_stats)
    return result_stats
