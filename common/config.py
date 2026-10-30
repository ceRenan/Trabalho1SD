import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _env(name: str, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return type(default)(value)


MCAST_GROUP = _env("DCIM_MCAST_GROUP", "224.1.1.1")
MCAST_PORT = _env("DCIM_MCAST_PORT", 5007)
MCAST_TTL = _env("DCIM_MCAST_TTL", 2)
MCAST_IFACE = _env("DCIM_MCAST_IFACE", "0.0.0.0")

GATEWAY_ID = _env("DCIM_GATEWAY_ID", "dcim-gateway")
GATEWAY_TCP_PORT = _env("DCIM_TCP_PORT", 7000)
GATEWAY_UDP_PORT = _env("DCIM_UDP_PORT", 7001)
DISCOVERY_INTERVAL_S = _env("DCIM_DISCOVERY_INTERVAL", 15.0)
LIVENESS_CHECK_S = 2.0
OFFLINE_FACTOR = 3.0
OFFLINE_MIN_S = 6.0
GRPC_TIMEOUT_S = 2.0
MEMORY_WINDOW_S = 24 * 3600
DATA_DIR = _env("DCIM_DATA_DIR", os.path.join(_ROOT, "data"))

SIM_TICK_S = 1.0
UDP_BUFFER = 65535

ADVERTISE_IP = _env("DCIM_ADVERTISE_IP", "")
