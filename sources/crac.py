import argparse
import random

import grpc

from common.util import setup_logging
from protos import datacenter_pb2 as pb
from protos import datacenter_pb2_grpc as pb_grpc
from sources.base_source import BaseSource, CommonControlServicer

HOT_AISLE_C = 35.0
SUPPLY_ALARM_DELTA = 5.0
HYSTERESIS_C = 1.0


class PrecisionAC(BaseSource):
    SOURCE_TYPE = pb.PRECISION_AC
    CONTROLLABLE = True

    def __init__(self, source_id, rack_id, interval_s, setpoint, fan_speed):
        super().__init__(source_id, rack_id, interval_s)
        self.setpoint_c = setpoint
        self.fan_speed_pct = fan_speed
        self.supply_air_c = setpoint
        self.return_air_c = setpoint + 10
        self.power_kw = 0.0
        self.alarm_active = False

    def simulate(self, dt):
        cooling = self.powered_on and not self.failure_simulated
        fan = self.fan_speed_pct if self.powered_on else 0
        if cooling:
            rate, target = 0.05 + 0.25 * fan / 100.0, self.setpoint_c
        else:
            rate, target = 0.03, HOT_AISLE_C
        self.supply_air_c += (target - self.supply_air_c) * min(1.0, rate * dt) + random.gauss(0, 0.05)
        delta_t = (8 + 6 * (1 - fan / 100.0)) if fan else 3.0
        self.return_air_c = self.supply_air_c + delta_t + random.gauss(0, 0.1)
        if not self.powered_on:
            self.power_kw = 0.0
        else:
            compressor = 0.0 if self.failure_simulated else 2.5 + 0.3 * max(0.0, self.return_air_c - self.setpoint_c)
            self.power_kw = 0.4 + 0.03 * fan + compressor

    def detect_event(self):
        if not self.powered_on:
            self.alarm_active = False
            return None
        limit = self.setpoint_c + SUPPLY_ALARM_DELTA
        if not self.alarm_active and self.supply_air_c > limit:
            self.alarm_active = True
            return (pb.ALARM_RAISED,
                    f"Temperatura de insuflamento elevada: {self.supply_air_c:.1f} C (limite {limit:.1f} C)")
        if self.alarm_active and self.supply_air_c < limit - HYSTERESIS_C:
            self.alarm_active = False
            return (pb.ALARM_CLEARED, f"Temperatura de insuflamento normalizada: {self.supply_air_c:.1f} C")
        return None

    def fill_payload(self, packet):
        packet.ac.CopyFrom(pb.AcReading(
            supply_air_c=round(self.supply_air_c, 2),
            return_air_c=round(self.return_air_c, 2),
            setpoint_c=self.setpoint_c,
            fan_speed_pct=self.fan_speed_pct,
            power_kw=round(self.power_kw, 3),
        ))

    def fill_settings(self, state):
        state.ac.CopyFrom(pb.AcSettings(setpoint_c=self.setpoint_c, fan_speed_pct=self.fan_speed_pct))

    def describe(self):
        return (f"insufl={self.supply_air_c:.1f}C retorno={self.return_air_c:.1f}C "
                f"setpoint={self.setpoint_c:.1f}C vent={self.fan_speed_pct}% pot={self.power_kw:.2f}kW")

    def register_grpc(self, server):
        pb_grpc.add_PrecisionAcServiceServicer_to_server(PrecisionAcServicer(self), server)

    def set_setpoint(self, celsius):
        with self.lock:
            self.setpoint_c = round(celsius, 1)
        msg = f"Setpoint ajustado para {celsius:.1f} C"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg

    def set_fan_speed(self, pct):
        with self.lock:
            self.fan_speed_pct = int(pct)
        msg = f"Velocidade do ventilador ajustada para {pct}%"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg


class PrecisionAcServicer(CommonControlServicer, pb_grpc.PrecisionAcServiceServicer):
    def SetSetpoint(self, request, context):
        if not 16.0 <= request.celsius <= 30.0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "setpoint deve estar entre 16 e 30 C")
        return self._reply(self.device.set_setpoint(request.celsius))

    def SetFanSpeed(self, request, context):
        if not 10 <= request.percent <= 100:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "ventilador deve estar entre 10 e 100 %")
        return self._reply(self.device.set_fan_speed(request.percent))


def main():
    parser = argparse.ArgumentParser(description="Fonte A - Ar-condicionado de precisao")
    parser.add_argument("--id", default="crac-01", help="identificador da fonte")
    parser.add_argument("--room", default="SALA-1", help="localizacao")
    parser.add_argument("--interval", type=int, default=5, help="periodo de envio (s)")
    parser.add_argument("--setpoint", type=float, default=22.0, help="setpoint inicial (C)")
    parser.add_argument("--fan", type=int, default=60, help="ventilador inicial (%%)")
    parser.add_argument("--grpc-port", type=int, default=0, help="porta gRPC (0 = automatica)")
    args = parser.parse_args()

    setup_logging(args.id)
    PrecisionAC(args.id, args.room, args.interval, args.setpoint, args.fan).run(args.grpc_port)


if __name__ == "__main__":
    main()
