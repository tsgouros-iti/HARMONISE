"""
Subscribe to ONE HARMONISE MQTT topic and print every time the value changes.

Install:
  pip install paho-mqtt requests

Notes:
- MQTT auth: username can be anything, password MUST be JWT.
- IMPORTANT: Keep the script running (loop_forever).
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests
import paho.mqtt.client as mqtt

# -------------------- Credentials (DEV ONLY) --------------------
os.environ["HARMO_CLIENT_ID"] = "harmo_planner"
os.environ["HARMO_CLIENT_SECRET"] = "B8ileXdg0Gzr9rVKblVpvTvLyS3dUjhL"
os.environ["HARMO_USERNAME"] = "planner"
os.environ["HARMO_PASSWORD"] = "jegyuFkzz8j6YCyT"
# ----------------------------------------------------------------

KEYCLOAK_TOKEN_URL = "https://apigw.harmonise.mapsgroup.it/realms/Harmonise/protocol/openid-connect/token"

MQTT_HOST = "apigw.harmonise.mapsgroup.it"
MQTT_PORT = 9883

# Pick ONE topic. Example: BESS SOH measured
TOPIC = "harmo/aic/#"
# You can try also (depending on what exists in your topic list):
# TOPIC = "harmo/aic/aic10/bess/soc/measured"
# TOPIC = "harmo/aic/aic10/bess/voltage/measured"
# TOPIC = "harmo/aic/aic10/bess/current/measured"

def get_jwt() -> str:
    payload = {
        "client_id": os.environ["HARMO_CLIENT_ID"],
        "client_secret": os.environ["HARMO_CLIENT_SECRET"],
        "grant_type": "password",
        "username": os.environ["HARMO_USERNAME"],
        "password": os.environ["HARMO_PASSWORD"],
    }
    r = requests.post(
        KEYCLOAK_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {r.text}")
    return token

def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

# Keep last-seen values to print only on change
last_value = None
last_raw = None

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] on_connect rc={rc}")
    if rc != 0:
        print("[MQTT] connect failed")
        return
    client.subscribe(TOPIC, qos=0)
    print(f"[MQTT] subscribed to {TOPIC}")

def on_subscribe(client, userdata, mid, granted_qos):
    # granted_qos=(128,) means SUBACK failure (unauthorized or invalid topic)
    if any(q == 128 for q in granted_qos):
        print(f"[MQTT] SUBACK mid={mid} granted_qos={granted_qos} -> FAILURE (not authorized / bad topic)")
    else:
        print(f"[MQTT] SUBACK mid={mid} granted_qos={granted_qos} -> OK")

def on_message(client, userdata, msg):
    global last_value, last_raw

    raw = msg.payload.decode("utf-8", errors="replace").strip()

    # Try parse JSON and extract a meaningful "value"
    value = None
    observed_at = None
    try:
        obj = json.loads(raw)
        # Common payload patterns in telemetry
        if isinstance(obj, dict):
            value = obj.get("value", None)
            observed_at = obj.get("observedAt") or obj.get("readAt")
    except Exception:
        pass

    # Fallback: if it's not JSON, treat entire payload as the value
    if value is None:
        value = raw

    # Print only if changed
    if value != last_value or raw != last_raw:
        last_value = value
        last_raw = raw
        print(f"[{_now_utc()}] {msg.topic}")
        print(f"  value      = {value}")
        if observed_at:
            print(f"  observedAt  = {observed_at}")
        print(f"  raw        = {raw}\n")

def main():
    jwt = get_jwt()

    client = mqtt.Client(client_id="harmonise-mqtt-subscriber")
    client.username_pw_set(username="demo", password=jwt)  # username arbitrary, password=JWT

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)

    print(f"[MQTT] connecting to {MQTT_HOST}:{MQTT_PORT} ... (waiting for messages)")
    client.loop_forever()

if __name__ == "__main__":
    main()
