# test_sync_listing.py
import json
import datetime as dt
from googleapiclient.errors import HttpError

from app.google_client import get_google_service
from app.config import load_listings
from app.sync import sync_listing


def main():
    print("🔧 TEST: sync_listing() con escritura real\n")

    # 1) leer listings del entorno
    try:
        listings = load_listings()
    except Exception as e:
        print(f"❌ Error leyendo LISTINGS_JSON: {e}")
        return

    # 2) usar solo el primer listing
    listing = listings[0]
    print(f"📌 Probando listing: {listing['name']}")
    print(f"📌 calendar_id: {repr(listing['mirror_calendar_id'])}")
    print(f"📌 url iCal: {listing['airbnb_ical_url']}", "\n")

    # 3) crear cliente Google
    service = get_google_service()

    # 4) probar un GET al calendario
    try:
        print("👉 Probando lectura de eventos del calendario...")
        service.events().list(calendarId=listing["mirror_calendar_id"], maxResults=1).execute()
        print("   ✅ Lectura correcta\n")
    except HttpError as e:
        print("   ❌ Error al leer el calendario:", e)
        return

    # 5) ejecutar sync_listing
    print("👉 Ejecutando sync_listing() ...")

    try:
        stats = sync_listing(service, listing)
        print("\n📌 RESULTADOS:")
        print(json.dumps(stats, indent=2))
    except Exception as e:
        print("❌ Error ejecutando sync_listing():", e)


if __name__ == "__main__":
    main()