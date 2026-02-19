#-------------------------------------------------------------------------------
# Name:        aprs.py
# Purpose:     APRS-IS Beacon + Radiosonde telemetry uploader
# Author:      9A4AM
#-------------------------------------------------------------------------------

import socket
import time
import threading
import configparser
from datetime import datetime

# =======================
# LOAD SETTINGS
# =======================
_stop = False

config = configparser.ConfigParser()
config.read("settings.ini")

APRS_UPLOAD = config.getboolean("APRS", "APRS_Upload", fallback=False)
APRS_CALL   = config.get("APRS", "APRS_Call", fallback="NOCALL")
APRS_PASS   = config.get("APRS", "APRS_Pass", fallback="-1")
APRS_SERVER = config.get("APRS", "APRS_Server", fallback="radiosondy.info")
APRS_PORT   = config.getint("APRS", "APRS_Port", fallback=14580)

APRS_LAT = config.getfloat("STATION", "STATION_Lat", fallback=0.0)
APRS_LON = config.getfloat("STATION", "STATION_Lon", fallback=0.0)

APRS_BEACON_ENABLE  = config.getboolean("APRS", "APRS_Beacon_Enable", fallback=False)
# APRS_BEACON_TEXT    = config.get("APRS", "APRS_Beacon", fallback="OWRX_Radiosonde uploader by 9A4AM")
APRS_BEACON_TEXT = "OWRX_Radiosonde uploader by 9A4AM"
APRS_BEACON_INTERVAL = config.getint("APRS", "APRS_Beacon_interval", fallback=600)

APRS_INTERVAL = config.getint("APRS", "APRS_Upload_interval", fallback=20)

# =======================
# INTERNAL STATE
# =======================

_sock = None
_last_beacon = 0
_last_sent = {}        # ser -> timestamp
_lock = threading.Lock()

# =======================
# APRS UTILS
# =======================

def aprs_latlon(lat, lon):
    lat_d = int(abs(lat))
    lat_m = (abs(lat) - lat_d) * 60
    lat_h = "N" if lat >= 0 else "S"

    lon_d = int(abs(lon))
    lon_m = (abs(lon) - lon_d) * 60
    lon_h = "E" if lon >= 0 else "W"

    return (
        f"{lat_d:02d}{lat_m:05.2f}{lat_h}",
        f"{lon_d:03d}{lon_m:05.2f}{lon_h}",
    )

def _connect():
    global _sock
    print(f"[APRS] Connecting...{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")

    _sock = socket.create_connection((APRS_SERVER, APRS_PORT), timeout=10)
    _sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    login = f"user {APRS_CALL} pass {APRS_PASS} vers OWRX-Radiosonde 1.0\n"
    _sock.sendall(login.encode())

    resp = _sock.recv(1024).decode(errors="ignore").strip()
    print(f"[APRS] Server: {resp}")

    # radiosondy.info not sendt "verified"
    if not resp.startswith("#"):
        raise Exception("APRS login failed")

    print(f"[APRS] Login OK... {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")


# =======================
# BEACON
# =======================

def _send_beacon():
    global _last_beacon

    if not APRS_BEACON_ENABLE:
        return

    now = time.time()
    if now - _last_beacon < APRS_BEACON_INTERVAL:
        return

    lat_s, lon_s = aprs_latlon(APRS_LAT, APRS_LON)

    packet = (
        f"{APRS_CALL}>APRS,TCPIP*:!"
        f"{lat_s}/{lon_s}`"
        f"{APRS_BEACON_TEXT}\n"
    )

    _sock.sendall(packet.encode())
    _last_beacon = now
    print(f"[APRS] Beacon sent...{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")

# =======================
# PUBLIC API
# =======================

def send_telemetry(data: dict):
    """
    Called from on_message()
    Required keys:
      ser, lat, lon, alt, time
      Optional keys: dir, speed, climb, freq, type, temp, hum, decoder_desc
    """

    if not APRS_UPLOAD:
        return

    global _sock

    with _lock:
        try:
            if _sock is None:
                _connect()

            _send_beacon()

            ser = data.get("ser")
            # Skip placeholder sonde IDs (DFM-xxxxxxxx, D-xxxxxxxx, Dxxxxxxxx)
            if not ser:
                return

            s = ser.strip().lower()

            # Skip placeholder / invalid DFM IDs:
            # D, Dxxxx, D-xxxx, DFM-xxxx
            if (
                s == "d"
                or (
                    (s.startswith("dfm-") or s.startswith("d-") or s.startswith("d"))
                    and "x" in s
                )
            ):
                return


            lat = data.get("lat")
            lon = data.get("lon")
            alt = data.get("alt")
            time_val = data.get("time")
            course_val = int(data.get("dir", 0)) % 360
            speed_ms = data.get("speed", 0)       # m/s
            climb = data.get("climb", 0)         # m/s
            freq = data.get("freq", 0)
            sonde_type = data.get("type", "")
            temp = data.get("temp")
            hum = data.get("humidity")
            decoder_desc = APRS_BEACON_TEXT
            payload_id = ser[:9]                  # max 9 char for APRS

            if not ser or lat is None or lon is None or alt is None or time_val is None:
                return

            now = time.time()
            last = _last_sent.get(ser, 0)
            if now - last < APRS_INTERVAL:
                return
            _last_sent[ser] = now

            lat_s, lon_s = aprs_latlon(lat, lon)
            alt_ft = int(alt * 3.28084)
            speed_kn = int(round(speed_ms * 1.94384))   #  knots
            climb_val = round(climb, 1)

            time_h = datetime.utcfromtimestamp(time_val).strftime("%H%M%S")

            # optional telemetry in comment
            temp_com = f"t={temp:.1f}C" if temp is not None else ""
            hum_com  = f"h={hum:.1f}%" if hum is not None else ""
            freq_com = f"{freq/1e6:.3f}MHz" if freq > 0 else ""
            type_com = f"Type={sonde_type}" if sonde_type else ""
            desc_com = decoder_desc if decoder_desc else ""

            comment = f"Clb={climb_val:.1f}m/s {temp_com} {hum_com} {freq_com} {type_com} {desc_com}".strip()

            # APRS pacet
            packet = (
                f"{APRS_CALL}>APRS,TCPIP*:;"
                f"{payload_id:<9}*{time_h}h"
                f"{lat_s}/{lon_s}O"
                f"{course_val:03d}/{speed_kn:03d}"
                f"/A={alt_ft:06d} "
                f"{comment}\n"
            )

            _sock.sendall(packet.encode())
            print(f"[APRS] Sent {ser}...{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")

        except Exception as e:
            print("[APRS] ERROR:", e)
            try:
                if _sock:
                    _sock.close()
            except:
                pass
            _sock = None


def _aprs_loop():
    global _sock

    while not _stop:
        try:
            if not APRS_UPLOAD:
                time.sleep(5)
                continue

            if _sock is None:
                _connect()

            if APRS_BEACON_ENABLE:
                _send_beacon()
            time.sleep(1)

        except Exception as e:
            print(f"[APRS] LOOP ERROR: {e}....{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
            try:
                if _sock:
                    _sock.close()
            except:
                pass
            _sock = None
            time.sleep(10)

def start():
    if not APRS_UPLOAD:
        print("[APRS] Upload disabled")
        return

    t = threading.Thread(target=_aprs_loop, daemon=True)
    t.start()
    print(f"[APRS] Background thread started....{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")

def stop():
    global _stop, _sock
    print("[APRS] Stopping APRS thread...")
    _stop = True
    try:
        if _sock:
            _sock.close()
    except:
        pass
    _sock = None

