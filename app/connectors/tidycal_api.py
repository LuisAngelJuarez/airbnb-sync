import os
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
from zoneinfo import ZoneInfo

from ..config import TIMEZONE

# =====================
# Config / Constantes
# =====================

TIDYCAL_API_BASE = "https://tidycal.com/api"

# Timeout para llamadas HTTP (segundos)
HTTP_TIMEOUT = 30

# Máximo de páginas al paginar bookings en TidyCal
MAX_TIDYCAL_PAGES = 50

# Zona horaria local (la que usas en tu negocio)
TZ_LOCAL = ZoneInfo(TIMEZONE)


def tidycal_headers() -> Dict[str, str]:
    """
    Headers para llamar a la API de TidyCal.
    Usa el token de acceso personal (PAT) en TIDYCAL_API_TOKEN.
    """
    token = os.environ.get("TIDYCAL_API_TOKEN")
    if not token:
        raise RuntimeError(
            "[tidycal] Falta la variable de entorno TIDYCAL_API_TOKEN "
            "(crea un Personal Access Token en TidyCal e impórtalo al entorno)."
        )
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        # Se usa también en POST/PATCH. En GET no hace daño.
        "Content-Type": "application/json",
    }


def tidycal_list_bookings_in_range(
    start_date: dt.date,
    end_date: dt.date,
) -> List[Dict[str, Any]]:
    """
    Llama a GET /bookings de TidyCal usando SOLO starts_at + page
    (la API es muy quisquillosa si combinas filtros y da 422).

    Luego filtra del lado cliente:
      - Ignora bookings canceladas (cancelled_at != null).
      - Ignora bookings fuera de [start_date, end_date).
    """
    headers = tidycal_headers()

    # Usar inicio del día en UTC con formato completo ISO 8601
    start_dt = dt.datetime.combine(start_date, dt.time.min).replace(tzinfo=dt.timezone.utc)
    start_str = start_dt.isoformat().replace("+00:00", "Z")

    print(f"[tidycal:list] Listando bookings desde {start_str} (filtro local hasta {end_date})")

    all_bookings: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "starts_at": start_str,
            "page": page,
            # NO mandamos ends_at, NO mandamos cancelled para evitar 422
        }

        try:
            resp = requests.get(
                f"{TIDYCAL_API_BASE}/bookings",
                headers=headers,
                params=params,
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            print(f"[tidycal:⚠ error] Error de red al listar bookings (page={page}): {e}")
            break

        if resp.status_code >= 400:
            print(f"[tidycal:⚠ error] status={resp.status_code} body={resp.text}")
            break

        data = resp.json() or {}
        page_items = data.get("data", []) or []
        print(f"  [tidycal:list] page={page} → {len(page_items)} bookings (sin filtrar)")

        if not page_items:
            break

        all_bookings.extend(page_items)

        page += 1
        if page > MAX_TIDYCAL_PAGES:
            print(f"[tidycal:list] Se alcanzó el límite de {MAX_TIDYCAL_PAGES} páginas, se detiene la paginación.")
            break

    # Filtro local por rango y no canceladas
    filtered: List[Dict[str, Any]] = []
    for b in all_bookings:
        starts_at_str = b.get("starts_at")
        if not starts_at_str:
            continue

        try:
            starts_at_dt = dt.datetime.fromisoformat(
                starts_at_str.replace("Z", "+00:00")
            )
        except ValueError:
            continue

        starts_date = starts_at_dt.date()
        if not (start_date <= starts_date < end_date):
            continue

        # Ignorar bookings canceladas
        if b.get("cancelled_at"):
            continue

        filtered.append(b)

    print(f"[tidycal:list] Total bookings filtradas en rango: {len(filtered)}")
    return filtered


def tidycal_create_booking_for_airbnb_slot(
    booking_type_id: int,
    starts_at_utc: str,
    contact_name: str,
    contact_email: str,
) -> bool:
    """
    Crea una booking en TidyCal para una noche de Airbnb.
    Devuelve True si se creó correctamente, False en caso de error.
    """
    headers = tidycal_headers()

    payload = {
        "starts_at": starts_at_utc,     # UTC, string tipo "2025-03-20T15:00:00Z"
        "name": contact_name,
        "email": contact_email,
        "timezone": TIMEZONE,           # ej. "America/Mexico_City"
        "booking_questions": [],        # si tu booking type exige preguntas, aquí las rellenas
    }

    try:
        resp = requests.post(
            f"{TIDYCAL_API_BASE}/booking-types/{booking_type_id}/bookings",
            headers=headers,
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        print(f"[tidycal:⚠ error] POST booking para slot {starts_at_utc}: {e}")
        return False

    if resp.status_code == 201:
        print(f"[tidycal:✓ create] Booking creada para slot {starts_at_utc}")
        return True

    # Manejo de algunos códigos típicos
    if resp.status_code == 409:
        print(
            f"[tidycal:conflict] El slot {starts_at_utc} no está disponible "
            f"(TidyCal devolvió 409 Conflict)."
        )
    else:
        print(
            f"[tidycal:⚠ error] Al crear booking para {starts_at_utc}: "
            f"status={resp.status_code} body={resp.text}"
        )
    return False


def tidycal_cancel_booking(booking_id: int, starts_at_utc: Optional[str]) -> bool:
    """
    Cancela una booking en TidyCal por ID.
    Devuelve True si se canceló (o ya estaba cancelada y se acepta), False en error real.
    """
    headers = tidycal_headers()
    reason = f"Cancelado por sincronización Airbnb (slot {starts_at_utc})"

    try:
        resp = requests.patch(
            f"{TIDYCAL_API_BASE}/bookings/{booking_id}/cancel",
            headers=headers,
            json={"reason": reason},
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        print(f"[tidycal:⚠ error] PATCH cancel booking_id={booking_id}: {e}")
        return False

    if resp.status_code == 200:
        print(f"[tidycal:✓ cancel] Booking {booking_id} ({starts_at_utc}) cancelada.")
        return True

    if resp.status_code == 400:
        print(
            f"[tidycal:info] Booking {booking_id} ({starts_at_utc}) ya estaba cancelada "
            f"(400 Bad Request)."
        )
        return True

    print(
        f"[tidycal:⚠ error] Al cancelar booking {booking_id}: "
        f"status={resp.status_code} body={resp.text}"
    )
    return False


def booking_date_from_starts_at_utc(starts_at: str) -> Optional[dt.date]:
    """
    Convierte un starts_at en UTC (string ISO) a fecha local (TIMEZONE).
    Devuelve solo la date, o None si no se puede parsear.
    """
    if not starts_at:
        return None

    try:
        dt_utc = dt.datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
    except ValueError:
        return None

    dt_local = dt_utc.astimezone(TZ_LOCAL)
    return dt_local.date()
