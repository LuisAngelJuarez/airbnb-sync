# test_sync_all.py  (más claro que test_sync_listing)
import json
from app.connectors.google_client import get_google_service
from app.config import load_listings
from app.sync import sync_all


def main():
    print("🔧 TEST: sync_all() con escritura real\n")

    try:
        listings = load_listings()
    except Exception as e:
        print(f"❌ Error leyendo LISTINGS_JSON: {e}")
        return

    if not listings:
        print("❌ No hay listings para sincronizar (LISTINGS_JSON vacío)")
        return

    service = get_google_service()

    print("👉 Ejecutando sync_all() ...\n")
    results = sync_all()

    print("\n📌 RESULTADOS GLOBALES:")
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
