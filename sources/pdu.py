import argparse
import math
import random
import time

from common.util import setup_logging
from protos import datacenter_pb2 as pb
from sources.base_source import BaseSource

HYSTERESIS_PCT = 5.0


class PDU(BaseSource):
    SOURCE_TYPE = pb.PDU
    CONTROLLABLE = False

    def __init__(self, source_id, rack_id, interval_s, base_kw, capacity_kw, overload_pct):
        super().__init__(source_id, rack_id, interval_s)
        self.base_kw = base_kw
        self.capacity_kw = capacity_kw
        self.overload_pct = overload_pct
        self.power_kw = base_kw
        self.voltage_v = 220.0
        self.current_a = 0.0
        self.energy_kwh = 0.0
        self.burst_left_s = 0.0
        self.overload_active = False
        self._t0 = time.monotonic()

    def simulate(self, dt):
        t = time.monotonic() - self._t0
        load = self.base_kw * (1 + 0.15 * math.sin(2 * math.pi * t / 300.0))
        if self.burst_left_s > 0:
            load *= 1.4
            self.burst_left_s -= dt
        elif random.random() < 0.003 * dt:
            self.burst_left_s = random.uniform(10, 20)
        self.power_kw = max(0.2, load + random.gauss(0, 0.08))
        self.voltage_v = 220.0 + random.gauss(0, 1.0)
        self.current_a = self.power_kw * 1000.0 / self.voltage_v
        self.energy_kwh += self.power_kw * dt / 3600.0

    def detect_event(self):
        pct = 100.0 * self.power_kw / self.capacity_kw
        if not self.overload_active and pct > self.overload_pct:
            self.overload_active = True
            return (pb.ALARM_RAISED, f"Sobrecarga eletrica: {self.power_kw:.2f} kW ({pct:.0f}% da capacidade)")
        if self.overload_active and pct < self.overload_pct - HYSTERESIS_PCT:
            self.overload_active = False
            return (pb.ALARM_CLEARED, f"Carga eletrica normalizada: {self.power_kw:.2f} kW ({pct:.0f}%)")
        return None

    def fill_payload(self, packet):
        packet.pdu.CopyFrom(pb.PduReading(
            power_kw=round(self.power_kw, 3),
            current_a=round(self.current_a, 2),
            voltage_v=round(self.voltage_v, 1),
            energy_kwh=round(self.energy_kwh, 5),
        ))

    def fill_settings(self, state):
        state.pdu.CopyFrom(pb.PduSettings(rated_capacity_kw=self.capacity_kw, overload_pct=self.overload_pct))

    def describe(self):
        return (f"pot={self.power_kw:.2f}kW corrente={self.current_a:.1f}A "
                f"tensao={self.voltage_v:.0f}V energia={self.energy_kwh:.4f}kWh")


def main():
    parser = argparse.ArgumentParser(description="Fonte C - PDU do rack")
    parser.add_argument("--rack", default="R01", help="identificador do rack")
    parser.add_argument("--id", default=None, help="identificador da fonte")
    parser.add_argument("--interval", type=int, default=5, help="periodo de envio (s)")
    parser.add_argument("--base-kw", type=float, default=5.0, help="carga media do rack (kW)")
    parser.add_argument("--capacity", type=float, default=10.0, help="capacidade nominal (kW)")
    parser.add_argument("--overload", type=float, default=80.0, help="percentual para alerta de sobrecarga")
    args = parser.parse_args()

    source_id = args.id or f"pdu-{args.rack}"
    setup_logging(source_id)
    PDU(source_id, args.rack, args.interval, args.base_kw, args.capacity, args.overload).run()


if __name__ == "__main__":
    main()
