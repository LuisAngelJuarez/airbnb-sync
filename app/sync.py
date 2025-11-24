import os
import datetime as dt
from typing import Dict, Any, List, Set, Optional
import hashlib

import requests
import icalendar
from zoneinfo import ZoneInfo
from googleapiclient.errors import HttpError

from .config import TIMEZONE, load_listings
from .google_client import get_google_service


# =====================
# Config / Constantes
# =====================

TIDYCAL_API_BASE = "https://tidycal.com/api"

# Timeout para llamadas HTTP (segundos)
HTTP_TIMEOUT = 30

# Máximo de páginas al paginar bookings en TidyCal
MAX_TIDYCAL_PAGES = 50

# Máximo de eventos al leer de Google Calendar
GOOGLE_CAL_MAX_RESULTS = 2500

# Zona horaria local (la que usas en tu negocio)
TZ_LOCAL = ZoneInfo(TIMEZONE)


# =========
# Helpers
# =========

def parse_time(hhmm: str) -> dt.time:
    """Convierte 'HH:MM' en un objeto datetime.time."""
    hour, minute = map(int, hhmm.split(":"))
    return dt.time(hour=hour, minute=minute)


def _tidycal_headers() -> Dict[str, str]:
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


def _collect_airbnb_slots_from_calendar(
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


def _booking_date_from_starts_at_utc(starts_at: str) -> Optional[dt.date]:
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

def _is_airbnb_google_event(summary: str | None, description: str | None) -> bool:
    """
    Determina si un evento de Google Calendar proviene de Airbnb,
    usando como criterio que TANTO el nombre (summary) como el correo
    (incluido normalmente en la descripción) contengan la palabra 'airbnb'.

    Esto evita confundir reservas directas con las que vienen de Airbnb.
    """
    s = (summary or "").lower()
    d = (description or "").lower()

    # Nombre con 'airbnb' Y correo/texto con 'airbnb'
    return ("airbnb" in s) and ("airbnb" in d)



# ============================
# TidyCal (API): listar / crear / cancelar
# ============================

def _list_tidycal_bookings_in_range(
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
    headers = _tidycal_headers()

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


def _create_tidycal_booking_for_airbnb_slot(
    booking_type_id: int,
    starts_at_utc: str,
    contact_name: str,
    contact_email: str,
) -> bool:
    """
    Crea una booking en TidyCal para una noche de Airbnb.
    Devuelve True si se creó correctamente, False en caso de error.
    """
    headers = _tidycal_headers()

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


def _cancel_tidycal_booking(booking_id: int, starts_at_utc: Optional[str]) -> bool:
    """
    Cancela una booking en TidyCal por ID.
    Devuelve True si se canceló (o ya estaba cancelada y se acepta), False en error real.
    """
    headers = _tidycal_headers()
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


# ============================
# Airbnb → TidyCal (sync principal por fechas)
# ============================

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
    airbnb_slots = _collect_airbnb_slots_from_calendar(cal, listing_cfg)

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
    all_bookings = _list_tidycal_bookings_in_range(start_date, end_date)

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
        day = _booking_date_from_starts_at_utc(starts_at)
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

        ok = _create_tidycal_booking_for_airbnb_slot(
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

        ok = _cancel_tidycal_booking(booking_id=booking_id, starts_at_utc=starts_at_utc)
        if ok:
            stats["cancelled"] += 1
        else:
            stats["errors"] += 1

    print(
        f"[airbnb→tidycal:done] {listing_cfg['name']}: "
        f"created={stats['created']} cancelled={stats['cancelled']} errors={stats['errors']}"
    )
    return stats


# ============================
# TidyCal → Airbnb (mirror para Airbnb vía Google)
# ============================

def _build_tidycal_key(ev: Dict[str, Any]) -> str:
    """
    Genera una llave estable para un evento (para no duplicar al espejar).
    Usa start, end y summary.
    """
    start = ev.get("start", {}).get("dateTime") or ev.get("start", {}).get("date") or ""
    end = ev.get("end", {}).get("dateTime") or ev.get("end", {}).get("date") or ""
    summary = ev.get("summary") or ""
    raw = f"{start}|{end}|{summary}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def mirror_tidycal_to_airbnb_calendar(
    service,
    src_calendar_id: str,
    dst_calendar_id: str,
    listing_name: str,
    days_ahead: int = 365,
) -> Dict[str, int]:
    """
    Lee eventos futuros del calendario PRINCIPAL (src_calendar_id, donde escribe TidyCal)
    y crea eventos "bloqueo" en dst_calendar_id para que Airbnb los vea como ocupados,
    como eventos de TODO EL DÍA.

    IMPORTANTE:
    - Aquí ya están mezcladas las reservas que vienen de TidyCal directo
      + las que importamos desde Airbnb a TidyCal (y que TidyCal a su vez mandó a Google).
    - Los eventos que provienen de Airbnb NO deben espejarse al mirror
      para no re-bloquear en Airbnb lo que ya viene de Airbnb.

    La detección de eventos que vienen de Airbnb se hace así:
      - El summary contiene la palabra 'airbnb'
      - Y ALGÚN correo asociado al evento contiene la palabra 'airbnb'
        (primero en attendees[].email, y además en correos que aparezcan en la descripción).
    """

    from typing import Optional, List
    import re

    def _extract_emails_from_attendees(attendees: Optional[List[Dict[str, Any]]]) -> List[str]:
        """Devuelve la lista de correos desde attendees[].email."""
        if not attendees:
            return []
        emails: List[str] = []
        for a in attendees:
            email = a.get("email")
            if isinstance(email, str):
                emails.append(email)
        return emails

    def _extract_emails_from_text(text: Optional[str]) -> List[str]:
        """Extrae correos de un texto usando una regex sencilla."""
        if not text:
            return []
        email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        return email_re.findall(text)

    def _is_airbnb_google_event(
        summary: Optional[str],
        description: Optional[str],
        attendees: Optional[List[Dict[str, Any]]],
    ) -> bool:
        """
        Determina si un evento de Google Calendar proviene de Airbnb,
        usando como criterio que:
          - el summary tenga 'airbnb'
          - y ALGÚN correo (attendees o texto) tenga 'airbnb'.
        """
        s = (summary or "").lower()
        if "airbnb" not in s:
            # Si el nombre ni siquiera menciona 'airbnb', lo consideramos directo.
            return False

        emails: List[str] = []
        emails.extend(_extract_emails_from_attendees(attendees))
        emails.extend(_extract_emails_from_text(description))

        has_airbnb_email = any("airbnb" in e.lower() for e in emails)

        return has_airbnb_email

    stats = {"created": 0, "deleted": 0, "errors": 0}

    print(f"[mirror] {listing_name}: TidyCal/Principal ({src_calendar_id}) → Mirror ({dst_calendar_id})")

    if src_calendar_id == dst_calendar_id:
        print(
            f"[mirror:⚠ abort] {listing_name}: "
            "src_calendar_id y dst_calendar_id son IGUALES. "
            "Se requieren 2 calendarios (principal y mirror)."
        )
        return stats

    now_utc = dt.datetime.now(dt.timezone.utc)
    now = now_utc.isoformat().replace("+00:00", "Z")
    limit = (now_utc + dt.timedelta(days=days_ahead)).isoformat().replace("+00:00", "Z")

    # 1) Leer eventos fuente (calendario principal, TidyCal ya escribió aquí)
    try:
        src_resp = service.events().list(
            calendarId=src_calendar_id,
            timeMin=now,
            timeMax=limit,
            singleEvents=True,
            orderBy="startTime",
            maxResults=GOOGLE_CAL_MAX_RESULTS,
        ).execute()
    except HttpError as e:
        print(f"[mirror:⚠ error] Al leer src {src_calendar_id}: {e}")
        stats["errors"] += 1
        return stats

    src_events = src_resp.get("items", [])
    print(f"[mirror:src] {listing_name}: encontrados {len(src_events)} eventos origen")

    # 2) Leer eventos ya espejados en el calendario destino
    try:
        dst_resp = service.events().list(
            calendarId=dst_calendar_id,
            timeMin=now,
            timeMax=limit,
            singleEvents=True,
            orderBy="startTime",
            maxResults=GOOGLE_CAL_MAX_RESULTS,
        ).execute()
    except HttpError as e:
        print(f"[mirror:⚠ error] Al leer dst {dst_calendar_id}: {e}")
        stats["errors"] += 1
        return stats

    dst_events = dst_resp.get("items", [])

    existing_by_key: Dict[str, Dict[str, Any]] = {}
    for ev in dst_events:
        ep = ev.get("extendedProperties", {}).get("private", {}) or {}
        if ep.get("source") == "tidycal" and ep.get("mirror_key"):
            existing_by_key[ep["mirror_key"]] = ev

    # 3) Crear espejos nuevos (o reemplazar existentes)
    src_keys = set()
    for idx, src in enumerate(src_events, start=1):
        status = src.get("status")
        raw_start = src.get("start", {})
        raw_end = src.get("end", {})
        summary = src.get("summary") or ""
        description_original = (src.get("description") or "")
        attendees = src.get("attendees") or []
        ep_src = src.get("extendedProperties", {}).get("private", {}) or {}

        print(
            f"  [mirror:src-ev] {listing_name} ev#{idx}: "
            f"status={status!r} start={raw_start} end={raw_end} summary={summary!r}"
        )

        # 0) Saltar eventos cancelados
        if status == "cancelled":
            print(f"  [mirror:skip-cancelled] {listing_name} ev#{idx}")
            continue

        # 1) Detectar eventos que vienen de Airbnb usando nombre + correo real
        comes_from_airbnb = _is_airbnb_google_event(
            summary=summary,
            description=description_original,
            attendees=attendees,
        )

        if comes_from_airbnb:
            print(
                f"  [mirror:skip-airbnb] {listing_name} ev#{idx}: "
                f"summary={summary!r} (detectado como importado de Airbnb por nombre+correo)"
            )
            continue
        else:
            print(
                f"  [mirror:take-direct] {listing_name} ev#{idx}: "
                f"summary={summary!r} (TRATADO como reserva directa / no-Airbnb)"
            )

        # ==== FECHA BASE DEL EVENTO ORIGEN ====
        src_start = raw_start or {}
        date_str = src_start.get("date")
        if not date_str:
            dt_str = src_start.get("dateTime")
            if dt_str:
                date_str = dt_str.split("T", 1)[0]

        if not date_str:
            print(f"  [mirror:skip-nodate] {listing_name} ev#{idx}: sin fecha legible, src_start={src_start}")
            continue

        try:
            day = dt.date.fromisoformat(date_str)
        except ValueError:
            print(f"  [mirror:skip-baddate] {listing_name} ev#{idx}: fecha inválida {date_str!r}")
            continue

        desired_start_date = day
        desired_end_date = day + dt.timedelta(days=1)
        start_display = f"{desired_start_date.isoformat()} (all-day)"
        # =====================================

        src_key = _build_tidycal_key(src)
        src_keys.add(src_key)

        # Usamos el summary/description del evento fuente, con nota de espejo
        summary_for_mirror = summary or "Reserva TidyCal"
        description = description_original.strip()
        if description:
            description += f"\n\nEspejo TidyCal para {listing_name}"
        else:
            description = f"Espejo TidyCal para {listing_name}"

        # 3.a Si ya existe mirror con ese key → borrar y recrear como all-day
        existing = existing_by_key.get(src_key)
        if existing:
            ev_id = existing.get("id")
            if ev_id:
                try:
                    service.events().delete(
                        calendarId=dst_calendar_id,
                        eventId=ev_id,
                    ).execute()
                    print(
                        f"  [mirror:🗑 replace] {listing_name} ev#{idx}: "
                        f"borrado mirror previo (mirror_key={src_key})"
                    )
                    stats["deleted"] += 1
                except HttpError as e:
                    print(f"  [mirror:⚠ error] delete previo {listing_name} ev#{idx}: {e}")
                    stats["errors"] += 1
                    # seguimos, intentamos crear de todos modos

        # 3.b Crear mirror all-day
        new_body = {
            "summary": f"[Block Airbnb] {summary_for_mirror}",
            "description": description,
            "start": {
                "date": desired_start_date.isoformat(),
            },
            "end": {
                "date": desired_end_date.isoformat(),
            },
            "transparency": "opaque",
            "extendedProperties": {
                "private": {
                    "source": "tidycal",
                    "listing_name": listing_name,
                    "mirror_key": src_key,
                    "src_calendar_id": src_calendar_id,
                }
            },
        }

        try:
            service.events().insert(calendarId=dst_calendar_id, body=new_body).execute()
            print(
                f"  [mirror:✓ create] {listing_name} ev#{idx}: {start_display} "
                f"({summary_for_mirror}) [slot all-day]"
            )
            stats["created"] += 1
        except HttpError as e:
            print(f"  [mirror:⚠ error] insert {listing_name} ev#{idx}: {e}")
            stats["errors"] += 1

    # 4) Borrar espejos que ya no existen en el principal (cancelaciones/eliminados)
    for ev in dst_events:
        ep = ev.get("extendedProperties", {}).get("private", {}) or {}
        if ep.get("source") != "tidycal":
            continue
        mirror_key = ep.get("mirror_key")
        if not mirror_key or mirror_key in src_keys:
            continue  # sigue vigente

        ev_id = ev.get("id")
        if not ev_id:
            continue

        start_display = (
            ev.get("start", {}).get("dateTime")
            or ev.get("start", {}).get("date")
        )
        try:
            print(
                f"  [mirror:🗑 delete-missing] {listing_name}: {start_display} "
                f"(mirror_key={mirror_key}) [slot all-day]"
            )
            service.events().delete(calendarId=dst_calendar_id, eventId=ev_id).execute()
            stats["deleted"] += 1
        except HttpError as e:
            print(f"  [mirror:⚠ error] delete mirror {listing_name}: {e}")
            stats["errors"] += 1

    print(
        f"[mirror:done] {listing_name}: created={stats['created']} "
        f"deleted={stats['deleted']} errors={stats['errors']}"
    )
    return stats


# ============================
# Orquestadores
# ============================

def sync_listing(service, listing_cfg: Dict[str, Any]) -> Dict[str, int]:
    """
    Flujo para UNA cabaña:
      1) Lee iCal de Airbnb y lo sincroniza con TidyCal (API) creando / cancelando bookings.
      2) Espeja TODO lo ocupado de tidycal_calendar_id hacia mirror_calendar_id
         como eventos all-day [Block Airbnb] para que Airbnb bloquee esas noches.
    """
    print("\n============================")
    print(f"Sincronizando: {listing_cfg['name']}")
    print(f"  → Calendario PRINCIPAL (TidyCal): {listing_cfg.get('tidycal_calendar_id')}")
    print(f"  → Calendario MIRROR (solo bloqueos para Airbnb): {listing_cfg.get('mirror_calendar_id', 'N/A')}")
    print("============================")

    # Calendario principal donde TidyCal escribe sus eventos (conexión nativa)
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

    result = {
        "created": airbnb_stats["created"] + tidy_stats["created"],
        "updated": 0,
        "deleted": airbnb_stats["cancelled"] + tidy_stats["deleted"],
        "errors": airbnb_stats["errors"] + tidy_stats["errors"],
    }

    print(f"[sync_listing] Final → {listing_cfg['name']}: {result}\n")
    return result


def sync_all() -> Dict[str, Dict[str, int]]:
    """
    Orquesta la sincronización para todas las cabañas definidas en listings.json.
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

    result: Dict[str, Dict[str, int]] = {}
    for listing in listings:
        result[listing["name"]] = sync_listing(service, listing)

    print("\n📌 RESULTADOS GLOBALES:")
    print(result)
    return result