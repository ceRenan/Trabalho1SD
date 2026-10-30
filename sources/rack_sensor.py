import argparse
import random

import grpc

from common.util import setup_logging
from protos import datacenter_pb2 as pb
from protos import datacenter_pb2_grpc as pb_grpc
from sources.base_source import BaseSource, CommonControlServicer

HYSTERESIS_C = 0.5


class RackSensor(BaseSource):
    SOURCE_TYPE = pb.RACK_SENSOR
    CONTROLLABLE = True

    def __init__(self, source_id, rack_id, interval_s, base_temp, threshold, spike_prob):
        super().__init__(source_id, rack_id, interval_s)
        self.base_temp = base_temp
        self.alert_threshold_c = threshold
        self.spike_prob = spike_prob
        self.temperature_c = base_temp
        self.humidity_pct = random.uniform(40, 55)
        self.spike_left_s = 0.0
        self.alert_active = False

    def simulate(self, dt):
        if self.spike_left_s > 0:
            self.temperature_c += 0.6 * dt
            self.spike_left_s -= dt
        else:
            self.temperature_c += (self.base_temp - self.temperature_c) * 0.05 * dt + random.gauss(0, 0.12)
            if random.random() < self.spike_prob * dt:
                self.spike_left_s = random.uniform(8, 15)
        self.humidity_pct += (50 - self.humidity_pct) * 0.02 * dt + random.gauss(0, 0.3)
        self.humidity_pct = min(80.0, max(20.0, self.humidity_pct))

    def detect_event(self):
        if not self.powered_on or self.failure_simulated:
            return None
        t, th = self.temperature_c, self.alert_threshold_c
        if not self.alert_active and t > th:
            self.alert_active = True
            return (pb.ALARM_RAISED, f"Temperatura acima do limite: {t:.1f} C (limiar {th:.1f} C)")
        if self.alert_active and t < th - HYSTERESIS_C:
            self.alert_active = False
            return (pb.ALARM_CLEARED, f"Temperatura normalizada: {t:.1f} C (limiar {th:.1f} C)")
        return None

    def fill_payload(self, packet):
        if self.failure_simulated:
            return
        packet.environment.CopyFrom(pb.RackEnvReading(
            temperature_c=round(self.temperature_c, 2),
            humidity_pct=round(self.humidity_pct, 1),
            alert_threshold_c=self.alert_threshold_c,
        ))

    def fill_settings(self, state):
        state.sensor.CopyFrom(pb.SensorSettings(alert_threshold_c=self.alert_threshold_c))

    def describe(self):
        return (f"temp={self.temperature_c:.1f}C umid={self.humidity_pct:.0f}% "
                f"limiar={self.alert_threshold_c:.1f}C")

    def register_grpc(self, server):
        pb_grpc.add_RackSensorServiceServicer_to_server(RackSensorServicer(self), server)

    def set_threshold(self, celsius):
        with self.lock:
            self.alert_threshold_c = round(celsius, 1)
        msg = f"Limiar de alerta ajustado para {celsius:.1f} C"
        self.emit_event(pb.STATE_CHANGED, msg)
        return msg


class RackSensorServicer(CommonControlServicer, pb_grpc.RackSensorServiceServicer):
    def SetAlertThreshold(self, request, context):
        if not 15.0 <= request.celsius <= 60.0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limiar deve estar entre 15 e 60 C")
        return self._reply(self.device.set_threshold(request.celsius))


def main():
    parser = argparse.ArgumentParser(description="Fonte B - Sensor de temperatura/umidade do rack")
    parser.add_argument("--rack", default="R01", help="identificador do rack")
    parser.add_argument("--id", default=None, help="identificador da fonte")
    parser.add_argument("--interval", type=int, default=5, help="periodo de envio (s)")
    parser.add_argument("--base-temp", type=float, default=24.0, help="temperatura media do rack (C)")
    parser.add_argument("--threshold", type=float, default=30.0, help="limiar de alerta (C)")
    parser.add_argument("--spike-prob", type=float, default=0.004, help="probabilidade de pico termico")
    parser.add_argument("--grpc-port", type=int, default=0, help="porta gRPC (0 = automatica)")
    args = parser.parse_args()

    source_id = args.id or f"sensor-{args.rack}"
    setup_logging(source_id)
    RackSensor(source_id, args.rack, args.interval, args.base_temp,
               args.threshold, args.spike_prob).run(args.grpc_port)


if __name__ == "__main__":
    main()
