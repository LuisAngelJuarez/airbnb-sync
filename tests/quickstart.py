# quickstart_test.py
import datetime
from googleapiclient.errors import HttpError

from app.google_client import get_google_service  # ⬅️ usamos tu función


def main():
    """Prueba de uso de Google Calendar usando get_google_service()."""
    try:
        service = get_google_service()

        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        print("Getting the upcoming 10 events from 'primary'")

        events_result = (
            service.events()
            .list(
                calendarId="primary",
                timeMin=now,
                maxResults=10,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )
        events = events_result.get("items", [])

        if not events:
            print("No upcoming events found.")
            return

        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))
            print(start, event["summary"])

    except HttpError as error:
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
