import datetime as dt
import hashlib
import re
from typing import Dict, Any, Optional, List, Set

from googleapiclient.errors import HttpError

# Máximo de eventos al leer de Google Calendar
GOOGLE_CAL_MAX_RESULTS = 2500


def build_tidycal_key(ev: Dict[str, Any]) -> str:
    """
    Genera una llave estable para un evento (para no duplicar al espejar).
    Usa start, end y summary.
    """
    start = ev.get("start", {}).get("dateTime") or ev.get("start", {}).get("date") or ""
    end = ev.get("end", {}).get("dateTime") or ev.get("end", {}).get("date") or ""
    summary = ev.get("summary") or ""
    raw = f"{start}|{end}|{summary}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


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
    """
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

    # Índice de eventos ya espejados por mirror_key
    existing_by_key: Dict[str, Dict[str, Any]] = {}
    for ev in dst_events:
        ep = ev.get("extendedProperties", {}).get("private", {}) or {}
        if ep.get("source") == "tidycal" and ep.get("mirror_key"):
            existing_by_key[ep["mirror_key"]] = ev

    # 3) Crear espejos nuevos (o reemplazar existentes si cambian)
    src_keys: Set[str] = set()
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

        src_key = build_tidycal_key(src)
        src_keys.add(src_key)

        # Usamos el summary/description del evento fuente, con nota de espejo
        summary_for_mirror = summary or "Reserva TidyCal"
        description = description_original.strip()
        if description:
            description += f"\n\nEspejo TidyCal para {listing_name}"
        else:
            description = f"Espejo TidyCal para {listing_name}"

        desired_summary = f"[Block Airbnb] {summary_for_mirror}"
        desired_start_str = desired_start_date.isoformat()
        desired_end_str = desired_end_date.isoformat()

        # 3.a Si ya existe mirror con ese key → dejarlo si ya coincide todo
        existing = existing_by_key.get(src_key)
        if existing:
            existing_start_date = existing.get("start", {}).get("date")
            existing_end_date = existing.get("end", {}).get("date")
            existing_summary = existing.get("summary")
            existing_description = (existing.get("description") or "").strip()
            existing_ep = existing.get("extendedProperties", {}).get("private", {}) or {}

            same_dates = (
                existing_start_date == desired_start_str and
                existing_end_date == desired_end_str
            )
            same_summary = existing_summary == desired_summary
            same_description = existing_description == description.strip()
            same_meta = (
                existing_ep.get("source") == "tidycal" and
                existing_ep.get("listing_name") == listing_name and
                existing_ep.get("mirror_key") == src_key and
                existing_ep.get("src_calendar_id") == src_calendar_id
            )

            if same_dates and same_summary and same_description and same_meta:
                print(
                    f"  [mirror:keep] {listing_name} ev#{idx}: "
                    f"mirror ya coincide, no se borra ni recrea."
                )
                # Ya está correcto, seguimos con el siguiente origen
                continue
            else:
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

        # 3.b Crear mirror all-day (si no existía, o si lo acabamos de borrar)
        new_body = {
            "summary": desired_summary,
            "description": description,
            "start": {
                "date": desired_start_str,
            },
            "end": {
                "date": desired_end_str,
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
