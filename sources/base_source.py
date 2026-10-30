import logging
import socket
import threading
import time
from concurrent import futures

import grpc
from google.protobuf.message import DecodeError

from common import config
from common.net import (create_multicast_receiver, disable_udp_connreset,
                        get_local_ip, now_ms)
from common.util import status_name
from protos import datacenter_pb2 as pb


class BaseSource:
    SOURCE_TYPE = pb.SOURCE_TYPE_UNSPECIFIED
    CONTROLLABLE = False

    def __init__(self, source_id: str, rack_id: str, interval_s: int):
        self.source_id = source_id
        self.rack_id = rack_id
        self.log = logging.getLogger(source_id)

        self.lock = threading.RLock()
        self.powered_on = True
        self.failure_simulated = False
        self.report_interval_s = int(interval_s)

        self.sequence = 0
        self.gateway_addr = None
        self._pending_events = []
        self._wakeup = threading.Event()
        self._reschedule = False
        self._running = threading.Event()

        self.data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.data_sock.bind(("0.0.0.0", 0))
        disable_udp_connreset(self.data_sock)

        self.ip = get_local_ip()
        self.port = self.data_sock.getsockname()[1]
        self.grpc_server = None

    def simulate(self, dt: float) -> None:
        pass

    def detect_event(self):
        return None

    def fill_payload(self, packet: pb.DataPacket) -> None:
        pass

    def fill_settings(self, state: pb.DeviceState) -> None:
        pass

    def describe(self) -> str:
        return ""

    def register_grpc(self, server: grpc.Server) -> None:
        pass

    def current_status(self) -> int:
        with self.lock:
            if self.failure_simulated:
                return pb.FAULT
            if not self.powered_on:
                return pb.STANDBY
            return pb.ONLINE

    def build_state(self) -> pb.DeviceState:
        with self.lock:
            state = pb.DeviceState(
                source_id=self.source_id,
                source_type=self.SOURCE_TYPE,
                status=self.current_status(),
                powered_on=self.powered_on,
                report_interval_s=self.report_interval_s,
                failure_simulated=self.failure_simulated,
            )
            self.fill_settings(state)
            return state

    def build_info(self) -> pb.SourceInfo:
        return pb.SourceInfo(
            source_id=self.source_id,
            source_type=self.SOURCE_TYPE,
            ip=self.ip,
            port=self.port,
            rack_id=self.rack_id,
            controllable=self.CONTROLLABLE,
            initial_state=self.build_state(),
        )

    def set_power(self, on: bool) -> str:
        with self.lock:
            self.powered_on = on
        msg = "Equipamento ligado e em operacao" if on else "Equipamento em modo standby (desligado)"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg

    def set_report_interval(self, seconds: int) -> str:
        with self.lock:
            self.report_interval_s = int(seconds)
            self._reschedule = True
        self._wakeup.set()
        msg = f"Intervalo de envio alterado para {seconds}s"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg

    def set_failure(self, active: bool) -> str:
        with self.lock:
            self.failure_simulated = active
        msg = "Falha operacional detectada no equipamento" if active else "Operacao normalizada apos falha"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg

    def emit_event(self, kind: int, description: str) -> None:
        with self.lock:
            self._pending_events.append((kind, description))
        self._wakeup.set()

    def build_packet(self, kind: int = pb.NO_EVENT, description: str = "") -> pb.DataPacket:
        with self.lock:
            self.sequence += 1
            packet = pb.DataPacket(
                source_id=self.source_id,
                source_type=self.SOURCE_TYPE,
                rack_id=self.rack_id,
                sequence=self.sequence,
                timestamp_ms=now_ms(),
                status=self.current_status(),
                event=kind,
                event_description=description,
            )
            if self.powered_on:
                self.fill_payload(packet)
            return packet

    def send_packet(self, kind: int = pb.NO_EVENT, description: str = "") -> None:
        with self.lock:
            addr = self.gateway_addr
        if addr is None:
            return
        packet = self.build_packet(kind, description)
        try:
            self.data_sock.sendto(packet.SerializeToString(), addr)
        except OSError as exc:
            self.log.warning("falha ao enviar UDP para %s: %s", addr, exc)
            return
        tag = "EVENTO" if kind != pb.NO_EVENT else "periodico"
        extra = f" | {description}" if description else ""
        self.log.info("UDP #%d (%s) [%s] %s%s", packet.sequence, tag,
                      status_name(packet.status), self.describe() if packet.WhichOneof("payload") else "-", extra)

    def _discovery_loop(self) -> None:
        sock = create_multicast_receiver()
        self.log.info("aguardando descoberta no grupo %s:%d", config.MCAST_GROUP, config.MCAST_PORT)
        while self._running.is_set():
            try:
                data, addr = sock.recvfrom(config.UDP_BUFFER)
            except OSError:
                continue
            request = pb.DiscoveryRequest()
            try:
                request.ParseFromString(data)
            except DecodeError:
                continue
            if not request.data_port or not request.reply_port:
                continue

            gateway_ip = request.gateway_ip if (request.gateway_ip and request.gateway_ip != "0.0.0.0") else addr[0]
            with self.lock:
                new_addr = (gateway_ip, request.data_port)
                changed = self.gateway_addr != new_addr
                self.gateway_addr = new_addr
            if changed:
                self.log.info("descoberta pelo Gateway '%s' -> dados para %s:%d",
                              request.gateway_id, gateway_ip, request.data_port)

            response = pb.DiscoveryResponse(info=self.build_info(), timestamp_ms=now_ms())
            try:
                self.data_sock.sendto(response.SerializeToString(), (gateway_ip, request.reply_port))
            except OSError as exc:
                self.log.warning("falha ao responder descoberta: %s", exc)
            if changed:
                self._wakeup.set()

    def _main_loop(self) -> None:
        last_tick = time.monotonic()
        next_send = time.monotonic()
        while self._running.is_set():
            now = time.monotonic()

            if now - last_tick >= config.SIM_TICK_S:
                with self.lock:
                    self.simulate(now - last_tick)
                    event = self.detect_event()
                last_tick = now
                if event:
                    self.emit_event(*event)

            with self.lock:
                events, self._pending_events = self._pending_events, []
                if self._reschedule:
                    next_send = min(next_send, now + self.report_interval_s)
                    self._reschedule = False
                interval = self.report_interval_s
            for kind, description in events:
                self.send_packet(kind, description)

            if now >= next_send:
                self.send_packet()
                next_send = now + interval

            timeout = max(0.0, min(last_tick + config.SIM_TICK_S, next_send) - time.monotonic())
            self._wakeup.wait(timeout)
            self._wakeup.clear()

    def _start_grpc(self, port: int) -> None:
        self.grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self.register_grpc(self.grpc_server)
        bound = self.grpc_server.add_insecure_port(f"0.0.0.0:{port}")
        if bound == 0:
            raise RuntimeError(f"nao foi possivel abrir a porta gRPC {port}")
        self.grpc_server.start()
        self.port = bound
        self.log.info("servidor gRPC escutando na porta %d", bound)

    def run(self, grpc_port: int = 0) -> None:
        self._running.set()
        if self.CONTROLLABLE:
            self._start_grpc(grpc_port)
        self.log.info("fonte '%s' iniciada (IP %s, porta %d, rack %s)",
                      self.source_id, self.ip, self.port, self.rack_id)
        threading.Thread(target=self._discovery_loop, name="discovery", daemon=True).start()
        try:
            self._main_loop()
        except KeyboardInterrupt:
            self.log.info("encerrando (Ctrl+C)")
        finally:
            self._running.clear()
            if self.grpc_server:
                self.grpc_server.stop(grace=0)
            self.data_sock.close()


class CommonControlServicer:
    def __init__(self, device: BaseSource):
        self.device = device

    def _reply(self, message: str) -> pb.CommandReply:
        self.device.log.info("gRPC -> %s", message)
        return pb.CommandReply(success=True, message=message, state=self.device.build_state())

    def GetState(self, request, context):
        return self.device.build_state()

    def SetPower(self, request, context):
        return self._reply(self.device.set_power(request.on))

    def SetReportInterval(self, request, context):
        if not 1 <= request.seconds <= 3600:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "periodo deve estar entre 1 e 3600 s")
        return self._reply(self.device.set_report_interval(request.seconds))

    def SimulateFailure(self, request, context):
        return self._reply(self.device.set_failure(request.active))
