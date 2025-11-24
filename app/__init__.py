from flask import Flask
from .routes import register_routes


def create_app() -> Flask:
    """Crea y configura la app Flask."""
    app = Flask(__name__)
    register_routes(app)
    return app