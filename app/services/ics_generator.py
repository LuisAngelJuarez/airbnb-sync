import datetime as dt
from typing import Dict, Any

import icalendar
from zoneinfo import ZoneInfo

from ..config import TIMEZONE
from ..connectors.tidycal_api import tidycal_list_bookings_in_range, booking_date_from_starts_at_utc

TZ_LOCAL = ZoneInfo(TIMEZONE)


def generate_ics_for_listing(listing_cfg: Dict[str, Any], days_ahead: int = 365) -> bytes:
    today = dt.datetime.now(TZ_LOCAL).date()
    end_date = today + dt.timedelta(days=days_ahead)

    booking_type_id = listing_cfg["tidycal_booking_type_id"]
    airbnb_email = listing_cfg.get("airbnb_contact_email", "")
    listing_name = listing_cfg["name"]

    bookings = tidycal_list_bookings_in_range(today, end_date)

    cal = icalendar.Calendar()
    cal.add("prodid", f"-//Airbnb Sync//{listing_name}//ES")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("method", "PUBLISH")
    cal.add("x-wr-calname", listing_name)

    now_utc = dt.datetime.now(dt.timezone.utc)

    for b in bookings:
        if b.get("booking_type_id") != booking_type_id:
            continue

        contact = b.get("contact") or {}
        if contact.get("email") == airbnb_email:
            # No re-bloquear lo que ya viene de Airbnb
            continue

        day = booking_date_from_starts_at_utc(b.get("starts_at"))
        if not day or day < today:
            continue

        event = icalendar.Event()
        event.add("uid", f"{b['id']}@airbnb-sync")
        event.add("dtstamp", now_utc)
        event.add("dtstart", day)
        event.add("dtend", day + dt.timedelta(days=1))
        event.add("summary", "Not available")
        event.add("transp", "OPAQUE")
        cal.add_component(event)

    return cal.to_ical()
