# routes.py
from flask import Blueprint, jsonify, Response

from .sync import sync_all
from .config import load_listings
from .services.ics_generator import generate_ics_for_listing

bp = Blueprint("main", __name__)


@bp.route("/", methods=["GET"])
def health():
    return "OK", 200


@bp.route("/sync", methods=["GET", "POST"])
def sync_handler():
    try:
        summary = sync_all()
        return jsonify({"status": "ok", "details": summary}), 200
    except Exception as e:
        print(f"[sync] Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@bp.route("/calendar/<slug>.ics", methods=["GET"])
def listing_calendar(slug: str):
    listings = load_listings()
    listing_cfg = next(
        (l for l in listings if l.get("info", {}).get("slug") == slug),
        None,
    )
    if not listing_cfg:
        return f"Listing '{slug}' no encontrado", 404

    ics_bytes = generate_ics_for_listing(listing_cfg)
    return Response(
        ics_bytes,
        status=200,
        mimetype="text/calendar",
    )


def register_routes(app):
    app.register_blueprint(bp)
