import os
import json
from typing import Optional, Dict, Any

try:
    import redis  # type: ignore
except ImportError:
    redis = None


def get_redis_client() -> Optional["redis.Redis"]:
    """
    Crea un cliente de Redis a partir de REDIS_URL.
    Si no hay REDIS_URL o no está instalado redis, devuelve None.
    """
    if redis is None:
        print("[redis] Paquete 'redis' no instalado, se omite guardado en Redis.")
        return None

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("[redis] REDIS_URL no definido, se omite guardado en Redis.")
        return None

    return redis.from_url(redis_url, decode_responses=True)


def save_snapshot_to_redis(snapshot: Dict[str, Any]) -> None:
    """
    Serializa y guarda el snapshot en Redis en la key fija
    'rec_hospitality:availability_snapshot'.
    """
    client = get_redis_client()
    if client is None:
        return

    key = "rec_hospitality:availability_snapshot"
    payload = json.dumps(snapshot, ensure_ascii=False)
    client.set(key, payload)
    print(f"[redis] Snapshot guardado en '{key}' ({len(payload)} bytes).")