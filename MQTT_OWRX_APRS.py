#-------------------------------------------------------------------------------
# Name:        OpenWebRX MQTT
# Purpose:     OpenWebRX MQTT → DB (Radiosonda) + Flask web + APRS_Upload
# Author:      9A4AM
# Created:     19.01.2026
#-------------------------------------------------------------------------------

import paho.mqtt.client as mqtt
import json
from flask import Flask, render_template, jsonify
from models import db, Radiosonda
from datetime import datetime
import time
import aprs
import configparser


DATABASE = "sqlite:///radiosonde.db"



# =======================
# SETTINGS.INI
# =======================

config = configparser.ConfigParser()
config.read("settings.ini")

PORT_WEBSERVER = int(
    config.get("SERVER", "port_webserver", fallback="80")
)


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE
db.init_app(app)

# Create database if not exist
with app.app_context():
    db.create_all()

# Cache for DFM APRS ID (use source ili rawid)
dfm_id_cache = {}

sonde_freq_cache = {}   # ser -> freq
rs41_type_cache = {}   # ser -> RS41-SGP / RS41-SG



# =======================
# MQTT CALLBACKS (OWRX)
# =======================

def on_connect(client, userdata, flags, rc):
    print("Connected to Mosquitto (OpenWebRX)")
    client.subscribe("openwebrx/radiosonde/#")

def on_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode())

        if raw.get("mode") != "SONDE":
            return

        print(f"Received OWRX SONDE data: {raw}")
        data = raw.get("data", {})

        # --- SONDE TYPE / MODEL ---
        sonde_type = data.get("type")
        sonde_model = clean_subtype(data.get("subtype"))

        # --- Define APRS ser / Payload_ID ---
        ser_value = None
        source_key = raw.get("source") or data.get("rawid") or data.get("id")

        # If not DFM, clear DFM cache
        if sonde_type != "DFM" and source_key in dfm_id_cache:
            del dfm_id_cache[source_key]

        # If decoder provides aprsid
        if data.get("aprsid"):
            ser_value = data["aprsid"]

        # M20
        elif sonde_type == "M20" and "rawid" in data and "id" in data:
            try:
                raw_part = data["rawid"].split("_")[1][:2]
                id_suffix = data["id"].split("-")[-1][-5:]
                ser_value = f"ME{raw_part}{id_suffix}"
                print(f"Generated M20 APRS ID: {ser_value}")
            except Exception as e:
                print("Error generating M20 APRS ID:", e)

        # DFM
        elif sonde_type == "DFM":
            if "id" in data and data["id"]:
                try:
                    dfm_num = ''.join(filter(str.isdigit, data["id"]))
                    ser_value = f"D{dfm_num}"

                    if source_key:
                        dfm_id_cache[source_key] = ser_value

                    print(f"Generated DFM APRS ID: {ser_value}")
                except Exception as e:
                    print("Error generating DFM APRS ID:", e)

            elif source_key in dfm_id_cache:
                ser_value = dfm_id_cache[source_key]
                print(f"Using cached DFM APRS ID: {ser_value}")

        # fallback
        if not ser_value:
            ser_value = data.get("id") or raw.get("source")

        if not ser_value:
            print("No sonde ID/aprsid, skipping message")
            return

        # time
        ts_raw = raw.get("timestamp", int(time.time() * 1000))
        ts = ts_raw // 1000
        vframe_value = ts_raw   # ← MILISEKUNDE


        with app.app_context():
            existing = Radiosonda.query.filter_by(ser=ser_value, vframe=vframe_value).first()
            if existing:
                local_time = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

                print(f"Duplicate sonde {ser_value} @ {local_time}, skipping")
                return

            hs_ms = raw.get("speed", data.get("vel_h"))
            vs_ms = raw.get("vspeed", data.get("vel_v"))

            hs_kmh = round(hs_ms / 3.6, 1) if hs_ms is not None else None
            climb_1d = round(vs_ms, 1) if vs_ms is not None else None

            freq_rx = raw.get("freq", data.get("tx_frequency")) or raw.get("freq")

            if ser_value in sonde_freq_cache:
                freq_final = sonde_freq_cache[ser_value]
            else:
                freq_final = freq_rx
                if freq_final:
                    sonde_freq_cache[ser_value] = freq_final
            real_id = data.get("id")
            # --- FIX RS41 subtype handling ---
            if sonde_type == "RS41" and sonde_model and sonde_model.startswith("RS41"):
                sonde_type = sonde_model

            # Keep last known RS41 subtype for this sonde
            if sonde_type == "RS41" and ser_value:
                last = Radiosonda.query.filter_by(ser=ser_value).order_by(Radiosonda.vframe.desc()).first()
                if last and last.type and last.type.startswith("RS41-"):
                    sonde_type = last.type



            new_entry = Radiosonda(
                lat=raw.get("lat", data.get("lat")),
                lon=raw.get("lon", data.get("lon")),
                alt=raw.get("altitude", data.get("alt")),
                speed=hs_kmh,
                dir=raw.get("course", data.get("heading")),
                type=sonde_model if sonde_type == "DFM" else sonde_type,
                ser=ser_value,
                time=ts,
                sats=raw.get("sats", data.get("sats")),
                freq=freq_final,
                rssi=None,
                vs=vs_ms,
                hs=raw.get("speed", data.get("vel_h")),
                climb=climb_1d,
                temp=data.get("temp") or data.get("weather", {}).get("temperature"),
                humidity=data.get("humidity") or data.get("weather", {}).get("humidity"),
                frame=data.get("frame"),
                vframe=vframe_value,
                launchsite=real_id,
                batt=data.get("batt") or data.get("battery")
            )

            db.session.add(new_entry)
            db.session.commit()
            print(f"Saved OWRX sonde {ser_value}")

            aprs.send_telemetry({
                "ser": ser_value,
                "lat": new_entry.lat,
                "lon": new_entry.lon,
                "alt": new_entry.alt,
                "time": new_entry.time,
                "dir": new_entry.dir,
                "speed": new_entry.speed,
                "climb": new_entry.climb,
                "temp": new_entry.temp,
                "humidity": new_entry.humidity,
                "batt": new_entry.batt,
                "freq": new_entry.freq,
                "type": new_entry.type,
            })

    except Exception as e:
        print(f"Error processing OWRX message: {e}")


# =======================
# MQTT CLIENT
# =======================

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)
client.loop_start()


# =======================
# FLASK ROUTES
# =======================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/data")
def get_data():
    subquery = db.session.query(
        Radiosonda.ser,
        db.func.max(Radiosonda.vframe).label("max_frame")
    ).group_by(Radiosonda.ser).subquery()

    data = db.session.query(Radiosonda).filter(
        db.and_(
            Radiosonda.ser == subquery.c.ser,
            Radiosonda.vframe == subquery.c.max_frame,
            Radiosonda.lat.isnot(None),
            Radiosonda.lon.isnot(None),
            Radiosonda.alt.isnot(None)
        )
    ).all()

    formatted_data = []
    for r in data:
        alt = round(r.alt, 1) if r.alt is not None else None
        climb = round(r.climb, 1) if r.climb is not None else None
        dir = int(round(r.dir)) if r.dir is not None else None
        time_str = datetime.fromtimestamp(r.time).strftime("%Y-%m-%d %H:%M:%S") if r.time else None
        freq_mhz = round(r.freq / 1e6, 3) if r.freq else None

        formatted_data.append({
            "lat": r.lat,
            "lon": r.lon,
            "alt": alt,
            "speed": r.speed,
            "dir": dir,
            "type": r.type,
            "ser": r.ser,
            "time": time_str,
            "sats": r.sats,
            "freq": freq_mhz,
            "rssi": r.rssi,
            "vs": r.vs,
            "hs": r.hs,
            "climb": r.climb,
            "temp": r.temp,
            "humidity": r.humidity,
            "frame": r.frame,
            "vframe": r.vframe,
            "launchsite": r.launchsite,
            "batt": r.batt
        })

    return jsonify(formatted_data)


def clean_subtype(subtype):
    if not subtype:
        return None
    return subtype.split(":", 1)[1] if ":" in subtype else subtype


@app.route("/map")
def map_view():
    return render_template("map.html")


# =======================
# MAIN
# =======================

if __name__ == "__main__":
    aprs.start()

    try:
        print("Starting Web server (IP:1191) + APRS + MQTT server for OpenWebRX SONDE by 9A4AM@2026 ...")
        app.run(debug=False, host="0.0.0.0", port=PORT_WEBSERVER)


    except KeyboardInterrupt:
        print("\nShutting down...")

    finally:
        aprs.stop()
        client.disconnect()
        client.loop_stop()
        print("Clean shutdown done")
