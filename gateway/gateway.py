import argparse
import threading

from common import config
from common.util import setup_logging
from gateway.control import GrpcController
from gateway.discovery import DiscoveryService
from gateway.registry import SourceRegistry
from gateway.storage import DataStore
from gateway.tcp_server import ClientServer
from gateway.udp_listener import DataListener


class Gateway:
    def __init__(self, tcp_port: int, udp_port: int, discovery_interval: float, persist: bool):
        self.stop_event = threading.Event()
        self.registry = SourceRegistry()
        self.store = DataStore(persist=persist)
        self.controller = GrpcController(self.registry)
        self.listener = DataListener(udp_port, self.registry, self.store)
        self.discovery = DiscoveryService(self.registry, self.listener.port, discovery_interval)
        self.server = ClientServer(tcp_port, self)

    def _monitor(self) -> None:
        while not self.stop_event.wait(config.LIVENESS_CHECK_S):
            self.registry.check_liveness()

    def run(self) -> None:
        self.listener.start()
        self.discovery.start()
        threading.Thread(target=self._monitor, name="monitor", daemon=True).start()
        try:
            self.server.serve_forever(self.stop_event)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop_event.set()
            self.discovery.stop()


def main():
    parser = argparse.ArgumentParser(description="Gateway DCIM do data center")
    parser.add_argument("--tcp-port", type=int, default=config.GATEWAY_TCP_PORT, help="porta TCP para o Cliente")
    parser.add_argument("--udp-port", type=int, default=config.GATEWAY_UDP_PORT, help="porta UDP para os dados")
    parser.add_argument("--discovery-interval", type=float, default=config.DISCOVERY_INTERVAL_S,
                        help="intervalo da redescoberta multicast (s)")
    parser.add_argument("--no-persist", action="store_true", help="desativa persistencia em CSV")
    parser.add_argument("-v", "--verbose", action="store_true", help="logs de depuracao")
    args = parser.parse_args()

    log = setup_logging("gateway", args.verbose)
    log.info("iniciando Gateway DCIM")
    Gateway(args.tcp_port, args.udp_port, args.discovery_interval, not args.no_persist).run()
    log.info("Gateway encerrado")


if __name__ == "__main__":
    main()
