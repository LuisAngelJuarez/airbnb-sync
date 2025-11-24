# routes.py
from flask import Blueprint, jsonify
from .sync import sync_all
from .google_client import get_google_service  # ⬅️ importa aquí

bp = Blueprint("main", __name__)


@bp.route("/", methods=["GET"])
def health():
    return "OK", 200


@bp.route("/debug-calendar", methods=["GET"])
def debug_calendar():
    """
    Ruta de debug para ver qué calendarios ve la credencial que usa Flask.
    """
    service = get_google_service()
    cal_list = service.calendarList().list().execute()
    items = [
        {"id": item["id"], "summary": item.get("summary")}
        for item in cal_list.get("items", [])
    ]
    return jsonify({"calendars": items}), 200


@bp.route("/sync", methods=["GET", "POST"])
def sync_handler():
    """
    Endpoint para lanzar la sincronización de todas las cabañas.
    """
    try:
        summary = sync_all()
        return jsonify({"status": "ok", "details": summary}), 200
    except Exception as e:
        print(f"[sync] Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def register_routes(app):
    app.register_blueprint(bp)
