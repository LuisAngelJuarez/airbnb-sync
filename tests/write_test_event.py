import datetime as dt

from googleapiclient.errors import HttpError
from zoneinfo import ZoneInfo

from app.connectors.google_client import get_google_service
from app.config import TIMEZONE

# 👇 Pega aquí el ID del calendario de PRUEBAS
CALENDAR_ID = "15f72f03e506685273495474c5e26284b6ab2667821e0f085db8c5723e2e2ce9@group.calendar.google.com"
# Ejemplo:
# CALENDAR_ID = "5bdee3fb30e8b7440cdacf435a95853b427c701f48c88fc4678f9f5287d08fae@group.calendar.google.com"


def main():
    service = get_google_service()

    tz = ZoneInfo(TIMEZONE)
    now = dt.datetime.now(tz)

    start = now + dt.timedelta(hours=1)
    end = start + dt.timedelta(hours=1)

    event_body = {
        "summary": "🔧 Prueba Airbnb Sync",
        "description": "Evento de prueba creado por el script write_test_event.py",
        "start": {
            "dateTime": start.isoformat(),
            "timeZone": TIMEZONE,
        },
        "end": {
            "dateTime": end.isoformat(),
            "timeZone": TIMEZONE,
        },
    }

    try:
        created = service.events().insert(
            calendarId=CALENDAR_ID,
            body=event_body,
        ).execute()

        print("✅ Evento creado correctamente.")
        print("ID:", created.get("id"))
        print("Link en Calendar:", created.get("htmlLink"))

        # Si quieres que se borre inmediatamente, descomenta esto:
        # service.events().delete(
        #     calendarId=CALENDAR_ID,
        #     eventId=created["id"],
        # ).execute()
        # print("🗑️ Evento de prueba eliminado.")

    except HttpError as e:
        print("❌ Error al crear evento:")
        print(e)


if __name__ == "__main__":
    main()
