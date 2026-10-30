import logging
import threading
import time
from dataclasses import dataclass, field

from common import config
from common.util import status_name, type_name
from protos import datacenter_pb2 as pb

log = logging.getLogger("gateway.registry")


@dataclass
class SourceEntry:
    info: pb.SourceInfo
    state: pb.DeviceState
    status: int
    last_seen: float = field(default_factory=time.time)
    last_packet: pb.DataPacket = None
    packets_received: int = 0
    packets_lost: int = 0
    last_sequence: int = 0

    @property
    def endpoint(self) -> str:
        return f"{self.info.ip}:{self.info.port}"

    def offline_timeout(self) -> float:
        interval = self.state.report_interval_s or 5
        return max(config.OFFLINE_MIN_S, config.OFFLINE_FACTOR * interval)


class SourceRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._entries = {}
        self._unknown_warned = set()

    def upsert_from_discovery(self, info: pb.SourceInfo, addr) -> None:
        copy = pb.SourceInfo()
        copy.CopyFrom(info)
        info = copy
        if not info.ip or info.ip == "0.0.0.0":
            info.ip = addr[0]
        state = pb.DeviceState()
        state.CopyFrom(info.initial_state)
        with self._lock:
            entry = self._entries.get(info.source_id)
            if entry is None:
                self._entries[info.source_id] = SourceEntry(
                    info=info, state=state, status=state.status)
                log.info("NOVA FONTE descoberta: %s (%s) em %s:%d rack=%s controlavel=%s",
                         info.source_id, type_name(info.source_type), info.ip, info.port,
                         info.rack_id, "sim" if info.controllable else "nao")
                return
            if (entry.info.ip, entry.info.port) != (info.ip, info.port):
                log.info("fonte %s mudou de endereco: %s -> %s:%d",
                         info.source_id, entry.endpoint, info.ip, info.port)
            entry.info = info
            entry.state = state

    def on_packet(self, packet: pb.DataPacket) -> bool:
        with self._lock:
            entry = self._entries.get(packet.source_id)
            if entry is None:
                if packet.source_id not in self._unknown_warned:
                    self._unknown_warned.add(packet.source_id)
                    log.warning("pacote de fonte nao descoberta '%s' ignorado", packet.source_id)
                return False

            if entry.last_sequence and packet.sequence > entry.last_sequence + 1:
                entry.packets_lost += packet.sequence - entry.last_sequence - 1
            elif packet.sequence <= entry.last_sequence:
                log.info("fonte %s reiniciou a sequencia (%d -> %d)",
                         packet.source_id, entry.last_sequence, packet.sequence)
            entry.last_sequence = packet.sequence

            if entry.status == pb.OFFLINE:
                log.info("fonte %s voltou a enviar dados (%s)", packet.source_id, status_name(packet.status))
            elif entry.status != packet.status:
                log.info("fonte %s: %s -> %s", packet.source_id,
                         status_name(entry.status), status_name(packet.status))
            entry.status = packet.status
            entry.state.status = packet.status
            entry.state.powered_on = packet.status != pb.STANDBY
            entry.state.failure_simulated = packet.status == pb.FAULT
            entry.last_seen = time.time()
            entry.last_packet = packet
            entry.packets_received += 1
            return True

    def update_state(self, source_id: str, state: pb.DeviceState) -> None:
        with self._lock:
            entry = self._entries.get(source_id)
            if entry:
                entry.state = state
                entry.status = state.status

    def mark_offline(self, source_id: str, reason: str) -> None:
        with self._lock:
            entry = self._entries.get(source_id)
            if entry and entry.status != pb.OFFLINE:
                entry.status = pb.OFFLINE
                log.warning("fonte %s OFFLINE: %s", source_id, reason)

    def check_liveness(self) -> None:
        now = time.time()
        with self._lock:
            for entry in self._entries.values():
                silence = now - entry.last_seen
                if entry.status != pb.OFFLINE and silence > entry.offline_timeout():
                    entry.status = pb.OFFLINE
                    log.warning("fonte %s OFFLINE: sem dados ha %.0f s", entry.info.source_id, silence)

    def get(self, source_id: str):
        with self._lock:
            entry = self._entries.get(source_id)
            if entry is None:
                return None
            return SourceEntry(info=entry.info, state=entry.state, status=entry.status,
                               last_seen=entry.last_seen, last_packet=entry.last_packet,
                               packets_received=entry.packets_received,
                               packets_lost=entry.packets_lost, last_sequence=entry.last_sequence)

    def ids(self):
        with self._lock:
            return sorted(self._entries)

    def status_of(self, source_id: str) -> int:
        with self._lock:
            entry = self._entries.get(source_id)
            return entry.status if entry else pb.STATUS_UNKNOWN

    def summaries(self):
        with self._lock:
            result = []
            for sid in sorted(self._entries):
                e = self._entries[sid]
                summary = pb.SourceSummary(
                    info=e.info, status=e.status, state=e.state,
                    last_seen_ms=int(e.last_seen * 1000),
                    packets_received=e.packets_received, packets_lost=e.packets_lost)
                if e.last_packet is not None:
                    summary.last_packet.CopyFrom(e.last_packet)
                result.append(summary)
            return result
