import time
import json
import gzip
import requests
import email.utils
import logging
from typing import Optional, List, Dict
import threading
import http.client
import urllib.parse
from datetime import datetime

SONDEHUB_STATION_URL = "https://api.v2.sondehub.org/listeners"
SONDEHUB_TELEMETRY_URL = "https://api.v2.sondehub.org/sondes/telemetry"

# -----------------------
# Global uploader instance
# -----------------------
_uploader: Optional["SondeHubUploader"] = None
_serial_queues: Dict[str, List[dict]] = {}
_serial_lock = threading.Lock()
_recent_packets: set = set()  # deduplikacija po (serial, frame)

# -----------------------
# Timestamp helper
# -----------------------
def now_str():
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


class SondeHubUploader:
    def __init__(
        self,
        enabled: bool,
        uploader_name: str = None,
        software_name: str = None,
        software_version: str = None,
        lat: float = None,
        lon: float = None,
        alt: Optional[float] = None,
        antenna: Optional[str] = None,
        email: Optional[str] = None,
        upload_interval: int = 10,
        timeout: int = 10,
        retries: int = 3,
        logger: Optional[logging.Logger] = None
    ):
        self.enabled = enabled
        self.uploader_name = uploader_name
        self.software_name = software_name
        self.software_version = software_version
        self.lat = lat
        self.lon = lon
        self.alt = alt
        self.antenna = antenna
        self.email = email or ""
        self.upload_interval = upload_interval
        self.timeout = timeout
        self.retries = retries
        self.logger = logger or logging.getLogger("SondeHub")
        self._last_station_upload = 0

    # -----------------------
    # Station upload
    # -----------------------
    def upload_station(self, force: bool = False):
        if not self.enabled:
            return
        now = time.time()
        if not force and (now - self._last_station_upload) < 21600:
            return

        payload = {
            "software_name": self.software_name,
            "software_version": self.software_version,
            "uploader_callsign": self.uploader_name,
            "uploader_position": [self.lat, self.lon, self.alt or 0],
            "uploader_radio": "",
            "uploader_antenna": self.antenna or "",
            "email": self.email,
            "mobile": False
        }

        headers = {
            "User-Agent": f"{self.software_name}-{self.software_version}",
            "Content-Type": "application/json",
            "Date": email.utils.formatdate(usegmt=True)
        }

        print(f"\n[{now_str()}] [SondeHub] Sending station info:")


        for attempt in range(self.retries):
            try:
                r = requests.put(
                    SONDEHUB_STATION_URL,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout
                )
            except Exception as e:
                self.logger.error("[%s] Station upload error: %s", now_str(), e)
                print(f"[{now_str()}] [SondeHub] Exception during station upload: {e}")
                continue

            if r.status_code == 200:
                self._last_station_upload = now
                self.logger.info("[%s] [SondeHub] SondeHub station uploaded", now_str())
                return
            elif r.status_code >= 500:
                print(f"[{now_str()}] [SondeHub] Server error, retrying...")
                continue
            else:
                print(f"[{now_str()}] [SondeHub] Unexpected status code {r.status_code}")
                self.logger.error("[%s] Station upload error", now_str())
                return

        print(f"[{now_str()}] [SondeHub] Station upload failed after all retries")
        self.logger.error("[%s] SondeHub station upload failed", now_str())

    # -----------------------
    # Telemetry upload (per-sonde)
    # -----------------------
    def upload_telemetry(self, packets: List[Dict]):
        if not self.enabled or not packets:
            return

        serial_map = {}
        for pkt in packets:
            serial = pkt.get("serial", "UNKNOWN")
            serial_map.setdefault(serial, []).append(pkt)

        for serial, batch in serial_map.items():
            batch.sort(key=lambda p: p.get("frame", 0))
            json_chunks = [json.dumps(pkt) for pkt in batch]
            json_data = "[" + ",".join(json_chunks) + "]"

            print(f"[{now_str()}] {json_data}")

            try:
                r = requests.put(
                    SONDEHUB_TELEMETRY_URL,
                    data=json_data,
                    headers={
                        "User-Agent": f"{self.software_name}-{self.software_version}",
                        "Content-Type": "application/json"
                    },
                    timeout=self.timeout
                )

                if r.status_code in (200, 400, 409):
                    print(f"[{now_str()}] [SondeHub] Telemetry uploaded for {serial}, packets={len(batch)}, status={r.status_code}")
                    self.logger.info("[%s] Telemetry uploaded (%d packets, status %d) for serial %s",
                                     now_str(), len(batch), r.status_code, serial)
                else:
                    print(f"[{now_str()}] [SondeHub] Upload failed for {serial}, status={r.status_code}")
                    self.logger.error("[%s] Telemetry upload failed (%d packets, status %d) for serial %s",
                                      now_str(), len(batch), r.status_code, serial)

            except Exception as e:
                print(f"[{now_str()}] [SondeHub] Exception uploading telemetry for {serial}: {e}")
                self.logger.error("[%s] Telemetry upload exception for serial %s: %s", now_str(), serial, e)


# -----------------------
# Public API
# -----------------------
def start(
    uploader: str,
    software_name: str,
    software_version: str,
    lat: float,
    lon: float,
    alt: Optional[float] = None,
    antenna: Optional[str] = None,
    email: Optional[str] = None,
    upload_interval: int = 5
):
    global _uploader
    if _uploader is not None:
        return

    _uploader = SondeHubUploader(
        enabled=True,
        uploader_name=uploader,
        software_name=software_name,
        software_version=software_version,
        lat=lat,
        lon=lon,
        alt=alt,
        antenna=antenna,
        email=email,
        upload_interval=upload_interval
    )

    _uploader.upload_station(force=True)

    threading.Thread(target=_periodic_worker, daemon=True).start()


def send_telemetry(packet: dict, batch_size=None):
    """RDZ-style per-sonde upload. batch_size argument je ignoriran."""
    global _serial_queues, _recent_packets, _uploader

    if not _uploader:
        return

    serial = packet.get("serial")
    frame = packet.get("frame")

    # --- deduplication ---
    key = (serial, frame)
    if key in _recent_packets:
        print(f"[{now_str()}] [SondeHub] Duplicate packet skipped: serial={serial}, frame={frame}")
        return
    _recent_packets.add(key)
    if len(_recent_packets) > 5000:
        _recent_packets.clear()

    # --- add to queue ---
    with _serial_lock:
        if serial not in _serial_queues:
            _serial_queues[serial] = []
        _serial_queues[serial].append(packet)

        # upload immediately per-sonde
        batch = _serial_queues[serial][:]
        _serial_queues[serial].clear()

    if batch:
        print(f"[{now_str()}] [SondeHub] Upload for serial {serial}, count={len(batch)}")
        _uploader.upload_telemetry(batch)


def flush_telemetry():
    """Send all remaining packets per serial RDZ-style."""
    global _serial_queues, _uploader
    if not _uploader:
        return
    with _serial_lock:
        for serial, queue in list(_serial_queues.items()):
            if queue:
                print(f"[{now_str()}] [SondeHub] Flushing batch for serial {serial}, count={len(queue)}")
                _uploader.upload_telemetry(queue[:])
        _serial_queues.clear()


def _periodic_worker():
    while True:
        uploader = _uploader
        if uploader is None:
            break

        time.sleep(uploader.upload_interval)
        flush_telemetry()

        uploader = _uploader
        if uploader is None:
            break

        uploader.upload_station()


def stop():
    global _uploader
    if _uploader:
        _uploader.enabled = False
    flush_telemetry()
    _uploader = None
