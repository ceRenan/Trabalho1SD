import logging
import socket
import struct
import sys
import time

from common import config

log = logging.getLogger(__name__)

_LEN_HEADER = struct.Struct("!I")
MAX_FRAME = 16 * 1024 * 1024


def now_ms() -> int:
    return int(time.time() * 1000)


def get_local_ip() -> str:
    if config.ADVERTISE_IP:
        return config.ADVERTISE_IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def disable_udp_connreset(sock: socket.socket) -> None:
    if sys.platform.startswith("win") and hasattr(socket, "SIO_UDP_CONNRESET"):
        try:
            sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        except (OSError, ValueError):
            pass


def _select_mcast_iface(iface: str) -> str:
    if iface and iface != "0.0.0.0":
        return iface
    return get_local_ip()


def create_multicast_receiver(group: str = None, port: int = None,
                              iface: str = None) -> socket.socket:
    group = group or config.MCAST_GROUP
    port = port or config.MCAST_PORT
    chosen_iface = _select_mcast_iface(iface or config.MCAST_IFACE)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass
    sock.bind(("", port))

    joined = False
    for candidate in dict.fromkeys([chosen_iface, "127.0.0.1", "0.0.0.0"]):
        try:
            mreq = struct.pack("=4s4s", socket.inet_aton(group), socket.inet_aton(candidate))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            joined = True
        except OSError:
            pass
    if not joined:
        mreq = struct.pack("=4sl", socket.inet_aton(group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    disable_udp_connreset(sock)
    return sock


def create_multicast_sender(ttl: int = None, iface: str = None) -> socket.socket:
    ttl = ttl or config.MCAST_TTL
    chosen_iface = _select_mcast_iface(iface or config.MCAST_IFACE)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(chosen_iface))
    except OSError:
        pass
    sock.bind(("0.0.0.0", 0))
    disable_udp_connreset(sock)
    return sock


def send_message(sock: socket.socket, message) -> None:
    payload = message.SerializeToString()
    sock.sendall(_LEN_HEADER.pack(len(payload)) + payload)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("conexao encerrada")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_message(sock: socket.socket, message_cls):
    (length,) = _LEN_HEADER.unpack(_recv_exact(sock, _LEN_HEADER.size))
    if length > MAX_FRAME:
        raise ConnectionError(f"mensagem excede limite ({length} bytes)")
    message = message_cls()
    message.ParseFromString(_recv_exact(sock, length))
    return message
