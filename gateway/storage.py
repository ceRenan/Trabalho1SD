import csv
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from common import config
from protos import datacenter_pb2 as pb

log = logging.getLogger("gateway.storage")

_TYPE_INFO = {
    pb.PRECISION_AC: ("precision_ac.csv", pb.AcReading),
    pb.RACK_SENSOR: ("rack_sensor.csv", pb.RackEnvReading),
    pb.PDU: ("pdu.csv", pb.PduReading),
}
_BASE_COLUMNS = ["timestamp_ms", "datetime", "source_id", "rack_id", "status", "event", "event_description"]


def metric_names(source_type: int):
    return [f.name for f in _TYPE_INFO[source_type][1].DESCRIPTOR.fields]


def extract_metrics(packet: pb.DataPacket) -> dict:
    which = packet.WhichOneof("payload")
    if which is None:
        return {}
    sub = getattr(packet, which)
    return {f.name: float(getattr(sub, f.name)) for f in sub.DESCRIPTOR.fields}


@dataclass
class Reading:
    timestamp_ms: int
    source_id: str
    source_type: int
    rack_id: str
    status: int
    event: int
    event_description: str
    metrics: dict = field(default_factory=dict)


class DataStore:
    def __init__(self, persist: bool = True, data_dir: str = config.DATA_DIR):
        self.persist = persist
        self.data_dir = data_dir
        self._lock = threading.Lock()
        self._by_source = {}
        if persist:
            os.makedirs(data_dir, exist_ok=True)
            self._load_recent()

    def add_packet(self, packet: pb.DataPacket) -> None:
        metrics = extract_metrics(packet)
        if not metrics or packet.source_type not in _TYPE_INFO:
            return
        reading = Reading(
            timestamp_ms=int(time.time() * 1000),
            source_id=packet.source_id,
            source_type=packet.source_type,
            rack_id=packet.rack_id,
            status=packet.status,
            event=packet.event,
            event_description=packet.event_description,
            metrics=metrics,
        )
        with self._lock:
            self._append(reading)
            if self.persist:
                self._write_csv(reading)

    def _append(self, reading: Reading) -> None:
        dq = self._by_source.setdefault(reading.source_id, deque())
        dq.append(reading)
        limit = reading.timestamp_ms - config.MEMORY_WINDOW_S * 1000
        while dq and dq[0].timestamp_ms < limit:
            dq.popleft()

    def readings(self, source_type: int = None, window_s: float = None):
        since = 0 if not window_s else int((time.time() - window_s) * 1000)
        with self._lock:
            result = []
            for dq in self._by_source.values():
                for r in dq:
                    if r.timestamp_ms >= since and (source_type is None or r.source_type == source_type):
                        result.append(r)
        result.sort(key=lambda r: r.timestamp_ms)
        return result

    def latest(self, source_type: int):
        with self._lock:
            return {sid: dq[-1] for sid, dq in self._by_source.items()
                    if dq and dq[-1].source_type == source_type}

    def _csv_path(self, source_type: int) -> str:
        return os.path.join(self.data_dir, _TYPE_INFO[source_type][0])

    def _write_csv(self, r: Reading) -> None:
        path = self._csv_path(r.source_type)
        names = metric_names(r.source_type)
        new_file = not os.path.exists(path)
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if new_file:
                    writer.writerow(_BASE_COLUMNS + names)
                writer.writerow([
                    r.timestamp_ms,
                    datetime.fromtimestamp(r.timestamp_ms / 1000).isoformat(timespec="seconds"),
                    r.source_id, r.rack_id,
                    pb.SourceStatus.Name(r.status), pb.EventKind.Name(r.event), r.event_description,
                ] + [r.metrics.get(n, "") for n in names])
        except OSError as exc:
            log.warning("falha ao gravar CSV %s: %s", path, exc)

    def _read_csv(self, source_type: int):
        path = self._csv_path(source_type)
        if not os.path.exists(path):
            return
        names = metric_names(source_type)
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    yield Reading(
                        timestamp_ms=int(row["timestamp_ms"]),
                        source_id=row["source_id"],
                        source_type=source_type,
                        rack_id=row["rack_id"],
                        status=pb.SourceStatus.Value(row["status"]),
                        event=pb.EventKind.Value(row["event"]),
                        event_description=row["event_description"],
                        metrics={n: float(row[n]) for n in names if row.get(n) not in (None, "")},
                    )
                except (KeyError, ValueError):
                    continue

    def _load_recent(self) -> None:
        since = int((time.time() - config.MEMORY_WINDOW_S) * 1000)
        count = 0
        for source_type in _TYPE_INFO:
            for r in self._read_csv(source_type):
                if r.timestamp_ms >= since:
                    self._append(r)
                    count += 1
        if count:
            log.info("%d leituras recarregadas dos CSVs em %s", count, self.data_dir)

    def history(self, source_type: int, source_id: str, limit: int, window_s: float = 0):
        if not self.persist:
            return self.readings(source_type, window_s)[-limit:]
        since = 0 if not window_s else int((time.time() - window_s) * 1000)
        with self._lock:
            rows = [r for r in self._read_csv(source_type)
                    if r.source_id == source_id and r.timestamp_ms >= since]
        return rows[-limit:]
