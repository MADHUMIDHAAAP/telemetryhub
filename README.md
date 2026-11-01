# TelemetryHub – Real-Time IoT Monitoring and Rule-Based Alerting Platform

Devices (ESP32 / built-in simulator) → HTTP or MQTT ingestion → time-series storage → WebSocket live dashboard → rule engine (e.g. temp > 8°C for 20 s) → Telegram alert, CSV export.

```
Sensors/Simulator ──HTTP POST /api/ingest──┐
ESP32 ──MQTT telemetryhub/<id>/data────────┤→ FastAPI → Rule engine → Alerts (DB + Telegram)
                                           │        ↓                    
                                           │   SQLite/Postgres ─→ REST + CSV
                                           └────── WebSocket /ws ──→ React-free live dashboard
```

## Run
    pip install -r requirements.txt
    uvicorn app.main:app --reload      # http://localhost:8000
    pytest -q
    docker build -t telemetryhub . && docker run -p 8000:8000 telemetryhub

## Deploy (Render, free)
Push to GitHub → Render → New → Blueprint → select repo (`render.yaml`). For persistent data set `DATABASE_URL` to a Postgres URL.

## Config (env vars)
`SIMULATOR=1|0`, `DATABASE_URL`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `MQTT_HOST` (+`MQTT_PORT`, topic `telemetryhub/<device>/data`, JSON `{"temperature":..,"humidity":..,"vibration":..}`).

## Real device
`curl -X POST $URL/api/ingest -H 'Content-Type: application/json' -d '{"device_id":"esp32-1","temperature":5.2,"humidity":60,"vibration":0.4}'`

## Problem → Solution → Impact
Problem: cold-storage and industrial equipment faults are noticed late. Solution: continuous telemetry + duration-based threshold rules (no ML) with online/offline tracking. Impact: faults surface within seconds and every reading is exportable for audit.
🔗 **Live demo:** (https://telemetryhub.onrender.com/)
> Free hosting-la irukkaradhaala mudhal load aaga 30-60 seconds aagalaam.
