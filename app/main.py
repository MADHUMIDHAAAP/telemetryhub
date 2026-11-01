"""TelemetryHub - real-time IoT telemetry ingestion, rule-based alerting, live dashboard."""
import os, asyncio, random, time, csv, io, json, threading, urllib.request, urllib.parse
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

DB = os.getenv("DATABASE_URL", "sqlite:///telemetryhub.db")
if DB.startswith("postgres://"):
    DB = DB.replace("postgres://", "postgresql://", 1)
engine = create_engine(DB, connect_args={"check_same_thread": False} if DB.startswith("sqlite") else {})
S = sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()
METRICS = ("temperature", "humidity", "vibration")

class Device(Base):
    __tablename__ = "devices"
    id = Column(String, primary_key=True); name = Column(String); location = Column(String, default=""); last_seen = Column(Float, default=0)
class Reading(Base):
    __tablename__ = "readings"
    id = Column(Integer, primary_key=True); device_id = Column(String, index=True); ts = Column(Float, index=True)
    temperature = Column(Float); humidity = Column(Float); vibration = Column(Float)
class Rule(Base):
    __tablename__ = "rules"
    id = Column(Integer, primary_key=True); metric = Column(String); op = Column(String); threshold = Column(Float); duration = Column(Integer)
class Alert(Base):
    __tablename__ = "alerts"
    id = Column(Integer, primary_key=True); ts = Column(Float); device_id = Column(String); rule_id = Column(Integer)
    message = Column(String); resolved = Column(Boolean, default=False)

clients, open_alerts, LOOP = set(), {}, None
lock = threading.Lock()

def notify(msg):
    tok, chat = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if tok and chat:
        def go():
            try: urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage?" + urllib.parse.urlencode({"chat_id": chat, "text": msg}), timeout=10)
            except Exception: pass
        threading.Thread(target=go, daemon=True).start()

def violates(r, v): return v > r.threshold if r.op == ">" else v < r.threshold

def ingest(did, t, h, v, ts=None):
    """Store a reading, evaluate rules. Returns (reading dict, new alerts)."""
    ts = ts or time.time(); vals = {"temperature": t, "humidity": h, "vibration": v}; new = []
    with lock, S() as s:
        d = s.get(Device, did)
        if not d: d = Device(id=did, name=did); s.add(d)
        d.last_seen = max(d.last_seen or 0, ts)
        s.add(Reading(device_id=did, ts=ts, **vals)); s.commit()
        for r in s.query(Rule).all():
            key = (did, r.id)
            if violates(r, vals[r.metric]):
                if key in open_alerts: continue
                rows = s.query(Reading).filter(Reading.device_id == did, Reading.ts >= ts - r.duration).order_by(Reading.ts).all()
                if len(rows) >= 2 and rows[0].ts <= ts - r.duration * 0.8 and all(violates(r, getattr(x, r.metric)) for x in rows):
                    msg = f"{did}: {r.metric} {r.op} {r.threshold} for {r.duration}s (now {vals[r.metric]:.1f})"
                    a = Alert(ts=ts, device_id=did, rule_id=r.id, message=msg); s.add(a); s.commit()
                    open_alerts[key] = a.id; new.append({"id": a.id, "ts": ts, "device_id": did, "message": msg, "resolved": False}); notify("ALERT " + msg)
            elif key in open_alerts:
                a = s.get(Alert, open_alerts.pop(key)); a.resolved = True; s.commit()
    return {"type": "reading", "device_id": did, "ts": ts, **vals}, new

async def broadcast(msgs):
    for c in list(clients):
        try:
            for m in msgs: await c.send_text(json.dumps(m))
        except Exception: clients.discard(c)

async def broadcast_one(r, alerts): await broadcast([r] + [{"type": "alert", **a} for a in alerts])

def push(r, alerts):
    if LOOP: asyncio.run_coroutine_threadsafe(broadcast_one(r, alerts), LOOP)

async def simulator():
    devs = [(f"{n}-{i:02d}", random.uniform(3, 5.5)) for n in ("ColdRoom", "Freezer", "Motor") for i in range(1, 4)]
    fault = {d: 0 for d, _ in devs}; n = 0
    while True:
        for d, base in devs:
            if fault[d] == 0 and random.random() < 0.006: fault[d] = random.randint(18, 40)
            f = fault[d] > 0; fault[d] = max(0, fault[d] - 1)
            t = base + (7 if f else 0) + random.gauss(0, .3); h = 60 + random.gauss(0, 2); v = (2.8 if f else .5) + abs(random.gauss(0, .1))
            await broadcast_one(*ingest(d, t, h, v))
        n += 1
        if n % 300 == 0:
            with S() as s: s.query(Reading).filter(Reading.ts < time.time() - 86400).delete(); s.commit()
        await asyncio.sleep(2)

def mqtt_worker():
    try:
        import paho.mqtt.client as mqtt
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        def on_msg(_c, _u, m):
            try:
                p = json.loads(m.payload); did = m.topic.split("/")[1]
                push(*ingest(did, float(p.get("temperature", 0)), float(p.get("humidity", 0)), float(p.get("vibration", 0))))
            except Exception: pass
        c.on_connect = lambda c, *a: c.subscribe("telemetryhub/+/data"); c.on_message = on_msg
        c.connect(os.environ["MQTT_HOST"], int(os.getenv("MQTT_PORT", 1883))); c.loop_forever()
    except Exception as e: print("MQTT disabled:", e)

@asynccontextmanager
async def lifespan(app):
    global LOOP; LOOP = asyncio.get_running_loop(); Base.metadata.create_all(engine)
    with S() as s:
        for a in s.query(Alert).filter_by(resolved=False): a.resolved = True
        if not s.query(Rule).count():
            s.add_all([Rule(metric="temperature", op=">", threshold=8, duration=20), Rule(metric="vibration", op=">", threshold=2, duration=20)])
        s.commit()
    open_alerts.clear(); tasks = []
    if os.getenv("SIMULATOR", "1") == "1": tasks.append(asyncio.create_task(simulator()))
    if os.getenv("MQTT_HOST"): threading.Thread(target=mqtt_worker, daemon=True).start()
    yield
    for t in tasks: t.cancel()

app = FastAPI(title="TelemetryHub", lifespan=lifespan)

class In(BaseModel):
    device_id: str; temperature: float = 0; humidity: float = 0; vibration: float = 0; ts: float | None = None
class RuleIn(BaseModel):
    metric: str; op: str; threshold: float; duration: int = 60

@app.post("/api/ingest")
async def api_ingest(p: In):
    r, a = ingest(p.device_id, p.temperature, p.humidity, p.vibration, p.ts); await broadcast_one(r, a); return {"ok": True, "alerts": len(a)}

@app.get("/api/devices")
def devices():
    now = time.time(); out = []
    with S() as s:
        for d in s.query(Device).order_by(Device.id):
            r = s.query(Reading).filter_by(device_id=d.id).order_by(Reading.ts.desc()).first()
            out.append({"id": d.id, "age": now - (d.last_seen or 0), "online": now - (d.last_seen or 0) < 15,
                        **({m: getattr(r, m) for m in METRICS} if r else {})})
    return out

@app.get("/api/readings/{did}")
def readings(did: str, minutes: int = 10):
    with S() as s:
        rows = s.query(Reading).filter(Reading.device_id == did, Reading.ts >= time.time() - minutes * 60).order_by(Reading.ts).all()
        return [{"ts": r.ts, **{m: getattr(r, m) for m in METRICS}} for r in rows]

@app.get("/api/alerts")
def alerts(limit: int = 50):
    with S() as s:
        return [{"id": a.id, "ts": a.ts, "device_id": a.device_id, "message": a.message, "resolved": a.resolved} for a in s.query(Alert).order_by(Alert.id.desc()).limit(limit)]

@app.get("/api/rules")
def rules():
    with S() as s: return [{"id": r.id, "metric": r.metric, "op": r.op, "threshold": r.threshold, "duration": r.duration} for r in s.query(Rule)]

@app.post("/api/rules")
def add_rule(r: RuleIn):
    if r.metric not in METRICS or r.op not in (">", "<"): raise HTTPException(422, "bad rule")
    with S() as s: s.add(Rule(**r.model_dump())); s.commit()
    return {"ok": True}

@app.delete("/api/rules/{rid}")
def del_rule(rid: int):
    with S() as s: s.query(Rule).filter_by(id=rid).delete(); s.commit()
    for k in [k for k in open_alerts if k[1] == rid]: open_alerts.pop(k)
    return {"ok": True}

@app.get("/api/stats")
def stats():
    with S() as s: return {"readings": s.query(Reading).count(), "devices": s.query(Device).count(), "open_alerts": len(open_alerts)}

@app.get("/api/export.csv")
def export(minutes: int = 60):
    def gen():
        buf = io.StringIO(); w = csv.writer(buf); w.writerow(["device_id", "timestamp", *METRICS]); yield buf.getvalue()
        with S() as s:
            for r in s.query(Reading).filter(Reading.ts >= time.time() - minutes * 60).order_by(Reading.ts):
                buf.seek(0); buf.truncate(); w.writerow([r.device_id, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r.ts)), r.temperature, r.humidity, r.vibration]); yield buf.getvalue()
    return StreamingResponse(gen(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=telemetry.csv"})

@app.websocket("/ws")
async def ws(w: WebSocket):
    await w.accept(); clients.add(w)
    try:
        while True: await w.receive_text()
    except WebSocketDisconnect: clients.discard(w)

HERE = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")
@app.get("/")
def index(): return FileResponse(os.path.join(HERE, "static", "index.html"))
