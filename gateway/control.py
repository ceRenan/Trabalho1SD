import logging
import threading

import grpc

from common import config
from common.util import type_name
from protos import datacenter_pb2 as pb
from protos import datacenter_pb2_grpc as pb_grpc

log = logging.getLogger("gateway.grpc")

_STUBS = {
    pb.PRECISION_AC: pb_grpc.PrecisionAcServiceStub,
    pb.RACK_SENSOR: pb_grpc.RackSensorServiceStub,
}

_ACTIONS = {
    pb.PRECISION_AC: {
        "set_power": "SetPower",
        "set_interval": "SetReportInterval",
        "set_setpoint": "SetSetpoint",
        "set_fan_speed": "SetFanSpeed",
        "set_failure": "SimulateFailure",
    },
    pb.RACK_SENSOR: {
        "set_power": "SetPower",
        "set_interval": "SetReportInterval",
        "set_threshold": "SetAlertThreshold",
        "set_failure": "SimulateFailure",
    },
}

_UNREACHABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class GrpcController:
    def __init__(self, registry):
        self.registry = registry
        self._lock = threading.Lock()
        self._channels = {}

    def _stub(self, entry):
        endpoint = entry.endpoint
        with self._lock:
            cached = self._channels.get(entry.info.source_id)
            if cached and cached[0] == endpoint:
                return cached[2]
            if cached:
                cached[1].close()
            channel = grpc.insecure_channel(endpoint)
            stub = _STUBS[entry.info.source_type](channel)
            self._channels[entry.info.source_id] = (endpoint, channel, stub)
            return stub

    def _handle_error(self, entry, exc: grpc.RpcError) -> str:
        code = exc.code()
        if code in _UNREACHABLE:
            self.registry.mark_offline(entry.info.source_id, f"gRPC {code.name}")
            return f"fonte inacessivel via gRPC ({code.name})"
        return f"erro gRPC {code.name}: {exc.details()}"

    def refresh_state(self, entry) -> bool:
        if entry.info.source_type not in _STUBS:
            return False
        try:
            state = self._stub(entry).GetState(pb.StateRequest(), timeout=1.0)
        except grpc.RpcError as exc:
            self._handle_error(entry, exc)
            return False
        self.registry.update_state(entry.info.source_id, state)
        return True

    def execute(self, entry, command: pb.ControlCommand) -> pb.CommandReply:
        action = command.WhichOneof("action")
        if not entry.info.controllable or entry.info.source_type not in _ACTIONS:
            return pb.CommandReply(success=False,
                                   message=f"{entry.info.source_id} ({type_name(entry.info.source_type)}) nao e controlavel")
        rpc_name = _ACTIONS[entry.info.source_type].get(action)
        if rpc_name is None:
            return pb.CommandReply(success=False,
                                   message=f"comando '{action}' nao suportado por {type_name(entry.info.source_type)}")

        request = getattr(command, action)
        rpc = getattr(self._stub(entry), rpc_name)
        log.info("gRPC %s.%s(%s) -> %s", entry.info.source_id, rpc_name,
                 str(request).strip().replace("\n", ", ") or "-", entry.endpoint)
        try:
            reply = rpc(request, timeout=config.GRPC_TIMEOUT_S)
        except grpc.RpcError as exc:
            msg = self._handle_error(entry, exc)
            log.warning("gRPC %s.%s falhou: %s", entry.info.source_id, rpc_name, msg)
            return pb.CommandReply(success=False, message=msg)
        self.registry.update_state(entry.info.source_id, reply.state)
        return reply
