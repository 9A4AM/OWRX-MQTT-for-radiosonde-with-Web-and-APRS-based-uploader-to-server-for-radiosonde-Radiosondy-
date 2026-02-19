#-------------------------------------------------------------------------------
# Name:        OpenWebRX+ MQTT
# Purpose:     OpenWebRX+ MQTT → DB (Radiosonda) + Flask web( with map) + APRS_Upload
#              + SondeHub_upload
# Author:      9A4AM
# Created:     19.01.2026 (rev.19.02.2026)
#-------------------------------------------------------------------------------

import paho.mqtt.client as mqtt
import json
from flask import Flask, render_template, jsonify
from models import db, Radiosonda
from datetime import datetime, timezone
import time
import aprs
import sondehub
import configparser


DATABASE = "sqlite:///radiosonde.db"

version = "1.0"
print(f"Version: {version}")


# =======================
# SETTINGS.INI
# =======================

config = configparser.ConfigParser()
config.read("settings.ini")

PORT_WEBSERVER = int(
    config.get("SERVER", "port_webserver", fallback="80")
)


# =======================
# SONDEHUB SETTINGS
# =======================

SONDEHUB_ENABLED = config.getboolean(
    "SONDEHUB", "SondeHub_Enable", fallback=False
)

SONDEHUB_UPLOADER = config.get(
    "SONDEHUB", "SondeHub_Uploader", fallback=None
)

# SONDEHUB_SOFTWARE_NAME = config.get(
#     "SONDEHUB", "SondeHub_Software_Name", fallback="Unknown"
# )

SONDEHUB_SOFTWARE_NAME = "OWRX+_9A4AM_Uploader"

# SONDEHUB_SOFTWARE_VERSION = config.get(
#     "SONDEHUB", "SondeHub_Software_Version", fallback="0.1"
# )

SONDEHUB_SOFTWARE_VERSION = version

SONDEHUB_UPLOAD_INTERVAL = config.getint(
    "SONDEHUB", "SondeHub_Upload_interval", fallback=30
)

SONDEHUB_LAT = config.getfloat(
    "STATION", "STATION_Lat", fallback=0
)

SONDEHUB_LON = config.getfloat(
    "STATION", "STATION_Lon", fallback=0
)

SONDEHUB_ALT = config.getfloat(
    "STATION", "STATION_Alt",
    fallback=0
)

SONDEHUB_ANTENNA = config.get(
    "SONDEHUB", "SondeHub_Antenna", fallback=None
)


# Optional e-mail
SONDEHUB_EMAIL = config.get(
    "SONDEHUB", "SondeHub_Email", fallback=None
)


# APRS Enable

APRS_ENABLED = config.getboolean("APRS", "APRS_Enable", fallback=False)

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
# Globalni set za privremenu deduplikaciju paketa
_recent_packets: set = set()





def init_uploaders():

    aprs.start()
    print("APRS uploader started")

    if SONDEHUB_ENABLED:
        sondehub.start(
            uploader=SONDEHUB_UPLOADER,
            software_name=SONDEHUB_SOFTWARE_NAME,
            software_version=SONDEHUB_SOFTWARE_VERSION,
            lat=SONDEHUB_LAT,
            lon=SONDEHUB_LON,
            alt=SONDEHUB_ALT,
            antenna=SONDEHUB_ANTENNA,
            email=SONDEHUB_EMAIL,
            # upload_interval=SONDEHUB_UPLOAD_INTERVAL
        )
        print("SondeHub uploader started")


init_uploaders()


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

        # print(f"Received OWRX SONDE data: {raw}")
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
                # print(f"Generated M20 APRS ID: {ser_value}")
            except Exception as e:
                print("Error generating M20 APRS ID:", e)

        # DFM
        elif sonde_type == "DFM":
            if "id" in data and data["id"]:
                try:
                    dfm_num = ''.join(filter(str.isdigit, data["id"]))
                    ser_value = f"D{dfm_num}"

                    if source_key and ser_value and ser_value != "D":
                        dfm_id_cache[source_key] = ser_value

                    # print(f"Generated DFM APRS ID: {ser_value}")
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
        s = ser_value.strip().lower()
        # Skip placeholder / invalid DFM IDs:
        # D, Dxxxx, D-xxxx, DFM-xxxx
        if (
            s == "d"
            or (
                (s.startswith("dfm-") or s.startswith("d-") or s.startswith("d"))
                and "x" in s
            )
        ):
            print(f"Skipping placeholder sonde ID: {ser_value}")
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

            if SONDEHUB_ENABLED:
                if all([new_entry.lat, new_entry.lon, new_entry.alt, new_entry.time, new_entry.ser]):
                    pkt = format_sondehub_packet(
                        new_entry,
                        SONDEHUB_UPLOADER,
                        SONDEHUB_SOFTWARE_NAME,
                        SONDEHUB_SOFTWARE_VERSION
                    )
                    if pkt:
                        # print(f"Packet SondeHub: {pkt}")
                        # sondehub.send_telemetry(pkt, batch_size=5)
                        sondehub.send_telemetry(pkt)
                else:
                    print(f"SondeHub skip (incomplete data): {new_entry.ser}")




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


@app.route("/track/<ser>")
def get_track(ser):
    points = (
        db.session.query(Radiosonda.lat, Radiosonda.lon, Radiosonda.alt)
        .filter(
            Radiosonda.ser == ser,
            Radiosonda.lat.isnot(None),
            Radiosonda.lon.isnot(None)
        )
        .order_by(Radiosonda.vframe.asc())
        .all()
    )

    return jsonify([
        {"lat": p.lat, "lon": p.lon, "alt": p.alt}
        for p in points
    ])



def clean_subtype(subtype):
    if not subtype:
        return None
    return subtype.split(":", 1)[1] if ":" in subtype else subtype


@app.route("/map")
def map_view():
    return render_template("map.html")



def detect_manufacturer(entry):
    t = (entry.type or "").upper()
    d = (getattr(entry, "device", "") or "").upper()
    s = (entry.ser or "").upper()

    # Vaisala
    if (
        "RS41" in t
        or "RS41-SG" in t
        or "RS41-SGP" in t
        or "RS92" in t
    ):
        return "Vaisala"


    # GRAW
    if any(x in t for x in ("DFM06", "DFM09", "DFM17", "DFM", "PS15", "PS-15")):
        return "Graw"


    # Meteomodem
    if "M10" in t or "M20" in t:
        return "Meteomodem"

    # Intermet
    if "IMET" in t:
        return "Intermet"

    # MTS01 (Meisei)
    if "MTS" in t or "MTS01" in t:
        return "Meisei"

    return "Other"


def get_sondehub_serial(entry):
    # RS41 / RS92 – is RAW
    if entry.type and entry.type.upper().startswith(("RS41", "RS41-SG", "RS41-SGP", "RS92")):
        return entry.ser

    raw = entry.launchsite or entry.ser or ""
    raw = raw.strip()

    # Meteomodem (M10 / M20)
    for p in ("M10-", "M20-"):
        if raw.startswith(p):
            return raw[len(p):]

    # GRAW / DFM variants
    for prefix in ("DFM06-", "DFM09-", "DFM17-", "DFM-", "PS15-", "PS-15-"):
        if raw.startswith(prefix):
            return raw[len(prefix):]

    # D-xxxx numeric
    if raw.startswith("D-"):
        return raw[2:]
    # Dxxxx numeric
    if raw.startswith("D") and raw[1:].isdigit():
        return raw[1:]

    # fallback: first char is digit
    if raw and raw[0].isdigit():
        return raw

    return None



def format_sondehub_packet(entry, uploader, sw_name, sw_ver):
    """
    Minimal SondeHub packet for all meteosondes including DFM and M20.
    Ensures uploader_position is a list [lat, lon, alt] for Receivers.
    """
    serial = get_sondehub_serial(entry)
    if any(v is None for v in [entry.lat, entry.lon, entry.alt, serial]):
        return None

    # datetime paketa → fallback na vframe
    pkt_datetime = getattr(entry, "datetime", None)
    if pkt_datetime is None:
        pkt_datetime = datetime.fromtimestamp(entry.vframe / 1000, tz=timezone.utc)

    pkt_datetime_str = pkt_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    time_received_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Calculate upload_time_delta (seconds as float)
    now_utc = datetime.now(timezone.utc)
    upload_time_delta = (pkt_datetime - now_utc).total_seconds()



    manuf = detect_manufacturer(entry)
    raw_type = getattr(entry, "type", "").upper()
    pkt_type = raw_type
    pkt_subtype = None

    if raw_type.startswith("RS41"):
        pkt_type = "RS41"
        pkt_subtype = raw_type
    elif raw_type.startswith("RS92"):
        pkt_type = "RS92"
        pkt_subtype = raw_type
    elif raw_type.startswith("M10"):
        pkt_type = "M10"
    elif raw_type.startswith("M20"):
        pkt_type = "M20"
    elif raw_type.startswith("DFM") or raw_type.startswith("PS"):
        pkt_type = "DFM"
        pkt_subtype = raw_type

    # Frame

    # if pkt_type == "DFM":
    #     pkt["frame"] = int(pkt_datetime.timestamp())  # Unix timestamp sekunde
    # else:
    #     pkt["frame"] = getattr(entry, "frame", 0)    # RS41, RS92, M10, M20


    # Minimalni paket
    pkt = {
        "software_name": sw_name,
        "software_version": sw_ver,
        "uploader_callsign": uploader,
        "uploader_position": [SONDEHUB_LAT, SONDEHUB_LON, SONDEHUB_ALT],
        "manufacturer": manuf,
        "type": pkt_type,
        "serial": serial,
        # "frame": pkt_frame,
        "frame": int(entry.frame),
        "datetime": pkt_datetime_str,
        "time_received": time_received_str,
        # "upload_time_delta": round(upload_time_delta, 3),
        "upload_time_delta": round(upload_time_delta, 3),
        "ref_position": "GPS",
        "ref_datetime": "GPS",
        "lat": float(entry.lat),
        "lon": float(entry.lon),
        "alt": float(entry.alt),
        "position": f"{float(entry.lat):.5f},{float(entry.lon):.5f}"  # string za Receivers
    }

    # if SONDEHUB_ANTENNA:
    #     pkt["uploader_antenna"] = SONDEHUB_ANTENNA

    if pkt_subtype:
        pkt["subtype"] = pkt_subtype

    # Ako imamo frekvenciju u entry, dodaj
    # frequency
    if entry.freq:
        if pkt_type in ["RS41", "RS92"]:
            pkt["frequency"] = round(entry.freq / 1e6, 4)  # 4 decimale
        else:
            pkt["frequency"] = round(entry.freq / 1e6, 6)  # Meteomodem

    # -----------------------------
    # Uploader position handling
    # -----------------------------
    if pkt_type in ["M10", "M20"]:
        # OVO OSTAVLJAMO - RADI
        pkt["uploader_position"] = [SONDEHUB_LAT, SONDEHUB_LON, SONDEHUB_ALT]

    else:
        # RS41 / RS92
        # pkt["uploader_position"] = f"{SONDEHUB_LAT:.5f},{SONDEHUB_LON:.5f}"
        # pkt["uploader_alt"] = float(SONDEHUB_ALT)
        pkt["uploader_position"] = [SONDEHUB_LAT, SONDEHUB_LON, SONDEHUB_ALT]

        if SONDEHUB_ANTENNA:
            pkt["uploader_antenna"] = SONDEHUB_ANTENNA

        # Signal metrike
        if hasattr(entry, "snr") and entry.snr is not None:
            pkt["snr"] = float(entry.snr)

        if hasattr(entry, "rssi") and entry.rssi is not None:
            pkt["rssi"] = float(entry.rssi)

        if entry.hs is not None:
            # pkt["vel_h"] = float(entry.hs)
            pkt["vel_h"] = float(entry.hs) / 3.6
        if entry.vs is not None:
            pkt["vel_v"] = float(entry.vs)
        if entry.dir is not None:
           pkt["heading"] = float(entry.dir)
        if entry.batt is not None:
            pkt["batt"] = float(entry.batt)

        pkt["tx_frequency"] = round(entry.freq / 1e6, 2)  # MHz s 2 decimale
        pkt["uploader_alt"] = float(SONDEHUB_ALT)       # obavezno


        if pkt_type == "DFM":

            # 1. datetime bez milisekundi
            pkt["datetime"] = pkt_datetime.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

            # 2. ref_datetime mora biti UTC
            pkt["ref_datetime"] = "UTC"

            # 3. uploader_position mora biti string
            pkt["uploader_position"] = f"{SONDEHUB_LAT:.5f},{SONDEHUB_LON:.5f}"
            pkt["uploader_alt"] = float(SONDEHUB_ALT)

        if pkt_type == "DFM":
            dfm_code_map = {
                "DFM17": "0xA",
                "DFM06": "0x6",
                "DFM09": "0x9",
                "PS15": "0xF",
                # dodaj ostale po potrebi
            }
            if pkt_subtype in dfm_code_map:
                pkt["dfmcode"] = dfm_code_map[pkt_subtype]




    return pkt















# =======================
# MAIN
# =======================

if __name__ == "__main__":


    try:
        print("Starting Web server + APRS + SondeHub + MQTT server + Map for OpenWebRX+ RADIOSONDE by 9A4AM@2026 ...")
        app.run(debug=False, host="0.0.0.0", port=PORT_WEBSERVER)

    finally:
        aprs.stop()
        if SONDEHUB_ENABLED:
            sondehub.stop()
        client.disconnect()
        client.loop_stop()


