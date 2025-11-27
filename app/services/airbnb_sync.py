import datetime as dt
from typing import Dict, Any, List, Set

import requests
import icalendar
from zoneinfo import ZoneInfo

from ..config import TIMEZONE
from ..connectors.tidycal_api import (
    tidycal_list_bookings_in_range,
    tidycal_create_booking_for_airbnb_slot,
    tidycal_cancel_booking,
    booking_date_from_starts_at_utc,
)

# Zona horaria local
TZ_LOCAL = ZoneInfo(TIMEZONE)

# Timeout para llamadas HTTP (segundos)
HTTP_TIMEOUT = 30


def parse_time(hhmm: str) -> dt.time:
    """Convierte 'HH:MM' en un objeto datetime.time."""
    hour, minute = map(int, hhmm.split(":"))
    return dt.time(hour=hour, minute=minute)


def fetch_airbnb_calendar(url: str) -> icalendar.Calendar:
    """Descarga y parsea el iCal de Airbnb."""
    print(f"[airbnb:fetch] Bajando iCal desde: {url}")
    resp = requests.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    print("[airbnb:fetch] iCal descargado correctamente")
    return icalendar.Calendar.from_ical(resp.text)


def _build_airbnb_daily_slots_for_component(
    component: icalendar.cal.Component,
    listing_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Convierte un VEVENT de varios días de Airbnb en "slots" diarios
    PARA TIDYCAL, SOLO si es una reserva real ("Reserved").

    Cada slot representa UNA NOCHE de estancia para esa cabaña.

    Devuelve una lista de dicts con:
      - 'start_local'  (datetime tz-aware en TIMEZONE)
      - 'start_utc_str' (string ISO8601 en UTC, ej. '2025-03-20T15:00:00Z')
      - 'date'         (date local, solo para logs y diff)
    """
    raw_summary = component.get("SUMMARY")
    raw_summary_str = str(raw_summary) if raw_summary is not None else ""
    raw_lower = raw_summary_str.lower()

    # 🔎 FILTRO: ignorar "Not available", "Airbnb blocked", etc.
    if "reserved" not in raw_lower:
        print(
            f"[airbnb:ignore] {listing_cfg['name']}: Ignorado {raw_summary_str!r} "
            f"({component.get('DTSTART').dt} → {component.get('DTEND').dt})"
        )
        return []

    dtstart = component.get("DTSTART").dt
    dtend = component.get("DTEND").dt

    # Normalizar a date en zona horaria local
    if isinstance(dtstart, dt.datetime):
        dtstart = dtstart.astimezone(TZ_LOCAL).date()
    if isinstance(dtend, dt.datetime):
        dtend = dtend.astimezone(TZ_LOCAL).date()

    if not isinstance(dtstart, dt.date) or not isinstance(dtend, dt.date):
        print(f"[airbnb:warn] DTSTART/DTEND inválidos: {dtstart} - {dtend}")
        return []

    # Generar días individuales: desde dtstart hasta el día antes de dtend
    days: List[dt.date] = []
    if dtend <= dtstart:
        days = [dtstart]
    else:
        cur = dtstart
        while cur < dtend:
            days.append(cur)
            cur += dt.timedelta(days=1)

    start_time = parse_time(listing_cfg["init_time"])

    slots: List[Dict[str, Any]] = []
    for day in days:
        # datetime local con la hora de check-in / inicio que tú definiste
        start_local = dt.datetime.combine(day, start_time, tzinfo=TZ_LOCAL)
        # La API de TidyCal requiere starts_at en UTC
        start_utc = start_local.astimezone(dt.timezone.utc).replace(microsecond=0)
        start_utc_str = start_utc.isoformat().replace("+00:00", "Z")

        slots.append(
            {
                "start_local": start_local,
                "start_utc_str": start_utc_str,
                "date": day,
            }
        )

    print(
        f"[airbnb:split] {listing_cfg['name']}: {dtstart} → {dtend} "
        f"({len(slots)} noches) summary='Reserved'"
    )
    return slots


def collect_airbnb_slots_from_calendar(
    cal: icalendar.Calendar,
    listing_cfg: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Recorre todo el iCal de Airbnb y devuelve un dict:
      key   = starts_at_utc (string)
      value = info del slot (start_local, date, etc.)

    Es la representación normalizada de TODAS las noches ocupadas en Airbnb
    para esa cabaña, vista como una booking por noche.
    """
    slots_by_start_utc: Dict[str, Dict[str, Any]] = {}

    for component in cal.walk("VEVENT"):
        daily_slots = _build_airbnb_daily_slots_for_component(component, listing_cfg)
        for slot in daily_slots:
            key = slot["start_utc_str"]
            # Si hubiera colisión, nos quedamos con el primero (no debería ocurrir)
            if key not in slots_by_start_utc:
                slots_by_start_utc[key] = slot

    print(
        f"[airbnb:slots] {listing_cfg['name']}: "
        f"{len(slots_by_start_utc)} noches normalizadas desde el iCal."
    )
    return slots_by_start_utc


def sync_airbnb_to_tidycal(
    cal: icalendar.Calendar,
    listing_cfg: Dict[str, Any],
    days_ahead: int = 365,
) -> Dict[str, int]:
    """
    Sincroniza el iCal de Airbnb de UNA cabaña hacia TidyCal usando la API.

    Estrategia:
      - Normaliza el iCal a "slots" diarios (una noche = un booking de TidyCal).
      - Lista las bookings actuales de TidyCal en un rango de fechas.
      - Toma SOLO las bookings que:
          * sean del booking_type de esta cabaña
          * y cuyo contact.email == airbnb_contact_email (marcamos así las importadas).
      - Hace un diff POR FECHA LOCAL (no por string exacta de starts_at):
          * Fechas en Airbnb pero no en TidyCal → crea bookings nuevas.
          * Fechas en TidyCal (marcadas Airbnb) que ya no están en el iCal → cancela.

    Requiere en listing_cfg:
      - 'tidycal_booking_type_id' (int)
      - 'airbnb_contact_email' (str)
    """
    stats = {"created": 0, "cancelled": 0, "errors": 0}

    print(f"[airbnb→tidycal] Sincronizando Airbnb → TidyCal para '{listing_cfg['name']}'")

    booking_type_id = listing_cfg.get("tidycal_booking_type_id")
    airbnb_contact_email = listing_cfg.get("airbnb_contact_email")

    if not booking_type_id:
        raise ValueError(
            f"[airbnb→tidycal] {listing_cfg['name']}: falta 'tidycal_booking_type_id' en listings.json"
        )
    if not airbnb_contact_email:
        raise ValueError(
            f"[airbnb→tidycal] {listing_cfg['name']}: falta 'airbnb_contact_email' en listings.json"
        )

    # 1) Normalizar Airbnb (iCal) a slots diarios
    airbnb_slots = collect_airbnb_slots_from_calendar(cal, listing_cfg)

    # ---- Airbnb: mapear por FECHA LOCAL ----
    # key = date (local), value = slot (tiene start_utc_str, date, etc.)
    airbnb_by_date: Dict[dt.date, Dict[str, Any]] = {}
    for slot in airbnb_slots.values():
        day = slot.get("date")
        if isinstance(day, dt.date) and day not in airbnb_by_date:
            airbnb_by_date[day] = slot

    airbnb_dates: Set[dt.date] = set(airbnb_by_date.keys())

    if airbnb_dates:
        min_date = min(airbnb_dates)
        max_date = max(airbnb_dates)
    else:
        # Si no hay nada en Airbnb, de todos modos revisamos un rango razonable
        today = dt.date.today()
        min_date = today
        max_date = today + dt.timedelta(days=days_ahead)

    # Expandimos un poco el rango para cubrir cambios cercanos
    today = dt.date.today()
    start_date = min(min_date, today)
    end_date = max(max_date, today + dt.timedelta(days=days_ahead))

    # 2) Leer bookings actuales en TidyCal en ese rango
    all_bookings = tidycal_list_bookings_in_range(start_date, end_date)

    # ---- TidyCal: mapear por FECHA LOCAL ----
    existing_by_date: Dict[dt.date, Dict[str, Any]] = {}
    for b in all_bookings:
        if b.get("booking_type_id") != booking_type_id:
            continue

        contact = b.get("contact") or {}
        email = contact.get("email")
        if email != airbnb_contact_email:
            continue

        starts_at = b.get("starts_at")
        day = booking_date_from_starts_at_utc(starts_at)
        if day is None:
            continue

        # Si hubiera doble booking el mismo día con el mismo email+type,
        # nos quedamos con el primero (caso raro, pero por si acaso).
        if day not in existing_by_date:
            existing_by_date[day] = b

    existing_dates: Set[dt.date] = set(existing_by_date.keys())

    print(
        f"[airbnb→tidycal] {listing_cfg['name']}: "
        f"{len(airbnb_dates)} noches (fechas) en Airbnb, "
        f"{len(existing_dates)} fechas con bookings Airbnb en TidyCal."
    )

    # 3) Diff POR FECHAS
    to_create_dates = sorted(airbnb_dates - existing_dates)
    to_cancel_dates = sorted(existing_dates - airbnb_dates)

    print(
        f"[airbnb→tidycal] {listing_cfg['name']}: "
        f"fechas por crear={to_create_dates}, fechas por cancelar={to_cancel_dates}"
    )

    # 4) Crear bookings nuevas
    for day in to_create_dates:
        slot = airbnb_by_date.get(day)
        if not slot:
            continue
        starts_at_utc = slot["start_utc_str"]

        print(
            f"[airbnb→tidycal:create] {listing_cfg['name']}: "
            f"creando booking para fecha {day} ({starts_at_utc})"
        )

        ok = tidycal_create_booking_for_airbnb_slot(
            booking_type_id=booking_type_id,
            starts_at_utc=starts_at_utc,
            contact_name=f"Reserva Airbnb - {listing_cfg['name']}",
            contact_email=airbnb_contact_email,
        )
        if ok:
            stats["created"] += 1
        else:
            stats["errors"] += 1

    # 5) Cancelar bookings que ya no están en Airbnb
    for day in to_cancel_dates:
        booking = existing_by_date.get(day) or {}
        booking_id = booking.get("id")
        starts_at_utc = booking.get("starts_at")

        if not booking_id:
            print(
                f"[airbnb→tidycal:warn] {listing_cfg['name']}: "
                f"no se encontró id de booking para fecha {day}, se omite cancelación."
            )
            continue

        print(
            f"[airbnb→tidycal:cancel] {listing_cfg['name']}: "
            f"cancelando booking_id={booking_id} para fecha {day} ({starts_at_utc})"
        )

        ok = tidycal_cancel_booking(booking_id=booking_id, starts_at_utc=starts_at_utc)
        if ok:
            stats["cancelled"] += 1
        else:
            stats["errors"] += 1

    print(
        f"[airbnb→tidycal:done] {listing_cfg['name']}: "
        f"created={stats['created']} cancelled={stats['cancelled']} errors={stats['errors']}"
    )
    return stats
