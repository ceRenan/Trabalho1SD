import logging
import socket
import threading

from google.protobuf.message import DecodeError

from common import config
from common.net import disable_udp_connreset
from protos import datacenter_pb2 as pb

log = logging.getLogger("gateway.udp")


class DataListener:
    def __init__(self, port: int, registry, store):
        self.registry = registry
        self.store = store
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        disable_udp_connreset(self.sock)
        self.port = self.sock.getsockname()[1]

    def start(self) -> None:
        threading.Thread(target=self._loop, name="udp-data", daemon=True).start()
        log.info("servidor UDP de dados escutando na porta %d", self.port)

    def _loop(self) -> None:
        while True:
            try:
                data, addr = self.sock.recvfrom(config.UDP_BUFFER)
            except OSError:
                continue
            packet = pb.DataPacket()
            try:
                packet.ParseFromString(data)
            except DecodeError:
                log.debug("datagrama invalido de %s", addr)
                continue
            if not packet.source_id:
                continue
            if not self.registry.on_packet(packet):
                continue
            self.store.add_packet(packet)
            if packet.event != pb.NO_EVENT:
                level = logging.WARNING if packet.event == pb.ALARM_RAISED else logging.INFO
                log.log(level, "EVENTO de %s: %s", packet.source_id, packet.event_description)
