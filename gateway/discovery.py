import logging
import threading

from google.protobuf.message import DecodeError

from common import config
from common.net import create_multicast_sender, get_local_ip, now_ms
from protos import datacenter_pb2 as pb

log = logging.getLogger("gateway.discovery")


class DiscoveryService:
    def __init__(self, registry, data_port: int, interval_s: float):
        self.registry = registry
        self.data_port = data_port
        self.interval_s = interval_s
        self.sock = create_multicast_sender()
        self.reply_port = self.sock.getsockname()[1]
        self._stop = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self._listen_responses, name="discovery-rx", daemon=True).start()
        threading.Thread(target=self._periodic_discovery, name="discovery-tx", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def send_discovery(self) -> None:
        request = pb.DiscoveryRequest(
            gateway_id=config.GATEWAY_ID,
            gateway_ip=get_local_ip(),
            data_port=self.data_port,
            reply_port=self.reply_port,
            timestamp_ms=now_ms(),
        )
        try:
            self.sock.sendto(request.SerializeToString(), (config.MCAST_GROUP, config.MCAST_PORT))
            log.debug("DiscoveryRequest enviado para %s:%d", config.MCAST_GROUP, config.MCAST_PORT)
        except OSError as exc:
            log.error("falha ao enviar multicast de descoberta: %s", exc)

    def _periodic_discovery(self) -> None:
        log.info("enviando descoberta multicast para %s:%d (respostas na porta UDP %d)",
                 config.MCAST_GROUP, config.MCAST_PORT, self.reply_port)
        while not self._stop.is_set():
            self.send_discovery()
            self._stop.wait(self.interval_s)

    def _listen_responses(self) -> None:
        while not self._stop.is_set():
            try:
                data, addr = self.sock.recvfrom(config.UDP_BUFFER)
            except OSError:
                continue
            response = pb.DiscoveryResponse()
            try:
                response.ParseFromString(data)
            except DecodeError:
                log.debug("resposta de descoberta invalida de %s", addr)
                continue
            if not response.info.source_id:
                continue
            self.registry.upsert_from_discovery(response.info, addr)
