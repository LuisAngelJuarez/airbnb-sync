from typing import Dict, Any, List, Set

from .config import load_listings
from .connectors.google_client import get_google_service

from .services.airbnb_sync import fetch_airbnb_calendar, sync_airbnb_to_tidycal
from .services.mirror_sync import mirror_tidycal_to_airbnb_calendar
from .services.availability_snapshot import get_blocked_nights_for_listing, build_availability_snapshot
from .connectors.redis_client import save_snapshot_to_redis


def sync_listing(service, listing_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flujo para UNA cabaña:
      1) Lee iCal de Airbnb y lo sincroniza con TidyCal (API) creando / cancelando bookings.
      2) Espeja TODO lo ocupado de tidycal_calendar_id hacia mirror_calendar_id
         como eventos all-day [Block Airbnb] para que Airbnb bloquee esas noches.
      3) Calcula noches bloqueadas desde TidyCal para esta cabaña (para el bot).
    """
    print("\n============================")
    print(f"Sincronizando: {listing_cfg['name']}")
    print(f"  → Calendario PRINCIPAL (TidyCal): {listing_cfg.get('tidycal_calendar_id')}")
    print(f"  → Calendario MIRROR (solo bloqueos para Airbnb): {listing_cfg.get('mirror_calendar_id', 'N/A')}")
    print("============================")

    busy_calendar_id = listing_cfg.get("tidycal_calendar_id")
    if not busy_calendar_id:
        raise ValueError(
            f"[sync_listing] {listing_cfg['name']}: falta 'tidycal_calendar_id' en listings.json"
        )

    # 1) Airbnb → TidyCal (API)
    cal = fetch_airbnb_calendar(listing_cfg["airbnb_ical_url"])
    airbnb_stats = sync_airbnb_to_tidycal(cal, listing_cfg)

    # 2) Principal (TidyCal) → mirror para Airbnb (all-day blocks)
    tidy_stats = {"created": 0, "deleted": 0, "errors": 0}
    mirror_calendar_id = listing_cfg.get("mirror_calendar_id")
    if mirror_calendar_id:
        print(
            f"[sync] {listing_cfg['name']}: mirror "
            f"{busy_calendar_id} → {mirror_calendar_id}"
        )
        tidy_stats = mirror_tidycal_to_airbnb_calendar(
            service=service,
            src_calendar_id=busy_calendar_id,   # de aquí lee (principal)
            dst_calendar_id=mirror_calendar_id, # aquí escribe all-day
            listing_name=listing_cfg["name"],
        )
    else:
        print(f"[sync] {listing_cfg['name']}: sin mirror_calendar_id, se omite espejo TidyCal → Airbnb.")

    # 3) Calcular noches bloqueadas desde TidyCal para el bot
    blocked_nights = get_blocked_nights_for_listing(listing_cfg)

    result_stats = {
        "created": airbnb_stats["created"] + tidy_stats["created"],
        "updated": 0,
        "deleted": airbnb_stats["cancelled"] + tidy_stats["deleted"],
        "errors": airbnb_stats["errors"] + tidy_stats["errors"],
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

    service = get_google_service()
    listings = load_listings()

    print("[sync_all] Listings cargados:")
    for l in listings:
        print(
            f"  - {l['name']} → "
            f"tidycal(principal)={l.get('tidycal_calendar_id', 'N/A')} "
            f"mirror={l.get('mirror_calendar_id', 'N/A')} "
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

        sync_result = sync_listing(service, listing)
        result_stats[name] = sync_result["stats"]
        blocked_by_slug[slug] = sync_result["blocked_nights"]

    # Construir snapshot para el bot
    snapshot = build_availability_snapshot(listings, blocked_by_slug)

    # Guardar en Redis (opcional, según REDIS_URL y redis instalado)
    try:
        save_snapshot_to_redis(snapshot)
    except Exception as e:
        print(f"[sync_all:⚠ error] No se pudo guardar snapshot en Redis: {e}")

    print("\n📌 RESULTADOS GLOBALES:")
    print(result_stats)
    return result_stats