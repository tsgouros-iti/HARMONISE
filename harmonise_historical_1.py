import csv
from datetime import datetime, timezone, timedelta
import requests

CLIENT_ID = "harmo_planner"
CLIENT_SECRET = "B8ileXdg0Gzr9rVKblVpvTvLyS3dUjhL"
USERNAME = "planner"
PASSWORD = "jegyuFkzz8j6YCyT"

KEYCLOAK_TOKEN_URL = "https://apigw.harmonise.mapsgroup.it/realms/Harmonise/protocol/openid-connect/token"
BASE = "https://apigw.harmonise.mapsgroup.it/scorpio/ngsi-ld/v1"
JSONLD_CONTEXT_LINK = '<https://apigw.harmonise.mapsgroup.it/schema/context.json>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'


def get_jwt() -> str:
    r = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_temporal_points(jwt: str, temporal_entity_id: str, start_z: str, end_z: str, limit: int = 5000) -> list:
    url = f"{BASE}/temporal/entities/{temporal_entity_id}"
    headers = {
        "Accept": "application/ld+json",
        "Link": JSONLD_CONTEXT_LINK,
        "Authorization": f"Bearer {jwt}",
    }
    params = {
        "attrs": "points",
        "timerel": "between",
        "timeAt": start_z,
        "endTimeAt": end_z,
        "limit": str(limit),
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("points", [])


def main():
    jwt = get_jwt()

    # One device + one property (your working one)
    ENTITY = "harmo:aic:aic10:bess:soc:measured"

    # one day from the previous week relative to 2026-02-04
    day = datetime(2026, 1, 28, tzinfo=timezone.utc)
    start = day.replace(hour=0, minute=0, second=0)
    end = day.replace(hour=23, minute=59, second=59)

    points = fetch_temporal_points(jwt, ENTITY, iso_z(start), iso_z(end), limit=5000)

    out_csv = f"history_{ENTITY.replace(':','_')}_{day.date()}.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["observedAt", "value", "readAt"])
        for p in points:
            w.writerow([p.get("observedAt"), p.get("value"), p.get("readAt")])

    print(f"[OK] points returned: {len(points)}")
    print(f"[OK] saved: {out_csv}")
    for p in points[:3]:
        print(" ", {"observedAt": p.get("observedAt"), "value": p.get("value")})


if __name__ == "__main__":
    main()
