# airbnb-sync

Sincroniza reservas de Airbnb con TidyCal y expone un endpoint ICS para que Airbnb bloquee las noches ocupadas por reservas directas.

## Cómo funciona

```
Airbnb iCal ──→ sync_airbnb_to_tidycal() ──→ TidyCal (crea/cancela bookings)

TidyCal bookings ──→ GET /calendar/<slug>.ics ──→ Airbnb suscrito a esa URL
```

1. **Airbnb → TidyCal**: Al correr `/sync`, descarga el iCal de cada cabaña de Airbnb y crea/cancela bookings en TidyCal para mantenerlos en sincronía.
2. **TidyCal → Airbnb**: El endpoint `/calendar/<slug>.ics` genera un ICS con las noches ocupadas por reservas directas en TidyCal (excluyendo las que vinieron de Airbnb para evitar ciclos).

---

## Requisitos

- Python 3.11+
- Cuenta en [TidyCal](https://tidycal.com) con Personal Access Token
- Redis (opcional, para snapshot de disponibilidad)

---

## Configuración

### 1. Instalar dependencias

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Variables de entorno

Crea un archivo `.env` en la raíz del proyecto:

```bash
export TIDYCAL_API_TOKEN=tu_token_de_tidycal
export LISTINGS_JSON='[...]'   # contenido de listings.json (ver abajo)
export REDIS_URL=redis://...   # opcional
```

### 3. Configurar listings.json

Copia `listings.example.json` como `listings.json` y rellena con tus datos reales:

```json
[
  {
    "name": "Cabaña2",
    "airbnb_ical_url": "https://www.airbnb.mx/calendar/ical/TU_ID.ics?s=...",
    "tidycal_booking_type_id": 1234567,
    "airbnb_contact_email": "airbnb-cabana2@tudominio.com",
    "init_time": "15:00",
    "finish_time": "11:00",
    "info": {
      "slug": "cabana2",
      "capacity": 2,
      "price_per_night": 800,
      ...
    }
  }
]
```

| Campo | Descripción |
|---|---|
| `airbnb_ical_url` | URL del iCal de Airbnb (Calendario → Exportar) |
| `tidycal_booking_type_id` | ID del tipo de booking en TidyCal para esta cabaña |
| `airbnb_contact_email` | Email ficticio que identifica bookings importados desde Airbnb |
| `init_time` | Hora de inicio del slot diario (HH:MM) |
| `info.slug` | Identificador corto usado en la URL del ICS (sin espacios ni acentos) |

---

## Correr en local

```bash
source .env && export LISTINGS_JSON="$(cat listings.json)" && python main.py
```

El servidor queda en `http://localhost:8080`.

---

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/` | Health check |
| `GET/POST` | `/sync` | Dispara la sincronización de todas las cabañas |
| `GET` | `/calendar/<slug>.ics` | ICS con noches bloqueadas por reservas directas |

Ejemplo:
```
http://localhost:8080/calendar/cabana2.ics
```

---

## Conectar con Airbnb

En cada listing de Airbnb:

1. Ve a **Calendario → Disponibilidad → Importar calendario**
2. Pega la URL pública de tu endpoint: `https://tu-dominio.com/calendar/<slug>.ics`
3. Airbnb actualizará el bloqueo automáticamente cada pocas horas

---

## Deploy con Docker

```bash
docker build -t airbnb-sync .
docker run -p 8080:8080 \
  -e TIDYCAL_API_TOKEN=... \
  -e LISTINGS_JSON="$(cat listings.json)" \
  airbnb-sync
```
