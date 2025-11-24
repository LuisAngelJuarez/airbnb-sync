# app/google_client.py
import os
import json

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth  # 👈 para default credentials (Cloud Run, SA, etc.)

from .config import SCOPES  # ["https://www.googleapis.com/auth/calendar"]


def _get_user_oauth_creds_from_env():
    """
    Devuelve credenciales OAuth de usuario usando SOLO variables de entorno.

    Requiere:
      - GOOGLE_OAUTH_CREDENTIALS: contenido JSON del credentials.json de OAuth
      - TOKEN_JSON (opcional la primera vez): token de usuario ya generado

    Flujo:
      1) Intenta leer TOKEN_JSON
      2) Si no es válido, lanza flujo OAuth (abre navegador) usando GOOGLE_OAUTH_CREDENTIALS
         -> imprime en consola el nuevo TOKEN_JSON para que lo copies a la variable de entorno.
    """
    creds = None

    # 1) Intentar cargar token desde ENV (si existe)
    token_env = os.getenv("TOKEN_JSON")
    if token_env:
        try:
            creds = Credentials.from_authorized_user_info(json.loads(token_env), SCOPES)
        except Exception:
            creds = None  # por si viene corrupto o mal formateado

    # 2) Si no hay credenciales válidas, lanzar flujo OAuth (solo tiene sentido en local)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # refrescar token automáticamente
            creds.refresh(Request())
        else:
            oauth_json = os.getenv("GOOGLE_OAUTH_CREDENTIALS")
            if not oauth_json:
                raise RuntimeError(
                    "⚠ No se encontraron credenciales OAuth en GOOGLE_OAUTH_CREDENTIALS "
                    "ni TOKEN_JSON válido. Define al menos GOOGLE_OAUTH_CREDENTIALS."
                )

            oauth_info = json.loads(oauth_json)

            flow = InstalledAppFlow.from_client_config(oauth_info, SCOPES)
            # Esto abre navegador: úsalo en tu máquina local, no en Cloud Run
            creds = flow.run_local_server(port=0)

        # 👉 IMPORTANTE:
        # Muestra el token para que lo copies y lo guardes en la variable de entorno TOKEN_JSON.
    return creds


def _get_service_account_creds_default():
    """
    Usa las credenciales por defecto del entorno (Application Default Credentials).

    - En Cloud Run: será la service account con la que corre el servicio.
    - Localmente: usará GOOGLE_APPLICATION_CREDENTIALS si lo tienes configurado.
    """
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds


def get_google_service():
    """
    Devuelve un cliente de Google Calendar con prioridad:

    1️⃣ Si existen variables de entorno de OAuth de usuario:
        - GOOGLE_OAUTH_CREDENTIALS (y opcionalmente TOKEN_JSON)
       -> Usa flujo OAuth de usuario (ideal para desarrollo local).

    2️⃣ En cualquier otro caso:
       -> Usa Application Default Credentials (service account),
          ideal para Cloud Run / producción.
    """

    use_oauth_env = os.getenv("GOOGLE_OAUTH_CREDENTIALS") is not None

    if use_oauth_env:
        # 🔹 Modo OAuth usuario vía variables de entorno
        creds = _get_user_oauth_creds_from_env()
    else:
        # 🔹 Modo service account / Cloud Run (default credentials)
        creds = _get_service_account_creds_default()

    return build("calendar", "v3", credentials=creds)
