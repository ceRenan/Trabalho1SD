import logging
import socket
import threading

from common.net import recv_message, send_message
from gateway.analytics import run_query
from protos import datacenter_pb2 as pb

log = logging.getLogger("gateway.tcp")


class ClientHandler(threading.Thread):
    def __init__(self, conn: socket.socket, addr, gateway):
        super().__init__(name=f"client-{addr[0]}:{addr[1]}", daemon=True)
        self.conn = conn
        self.addr = addr
        self.gw = gateway

    def run(self) -> None:
        log.info("cliente conectado: %s:%d", *self.addr)
        try:
            while True:
                request = recv_message(self.conn, pb.ClientRequest)
                response = self.dispatch(request)
                response.request_id = request.request_id
                send_message(self.conn, response)
        except (ConnectionError, OSError):
            pass
        finally:
            self.conn.close()
            log.info("cliente desconectado: %s:%d", *self.addr)

    def dispatch(self, request: pb.ClientRequest) -> pb.GatewayResponse:
        kind = request.WhichOneof("body")
        try:
            if kind == "list_sources":
                return self._list_sources(request.list_sources)
            if kind == "command":
                return self._command(request.command)
            if kind == "query":
                return self._query(request.query)
            return pb.GatewayResponse(success=False, error="requisicao vazia ou desconhecida")
        except Exception as exc:
            log.exception("erro ao processar requisicao %s", kind)
            return pb.GatewayResponse(success=False, error=f"erro interno: {exc}")

    def _list_sources(self, req: pb.ListSourcesRequest) -> pb.GatewayResponse:
        if req.refresh:
            for sid in self.gw.registry.ids():
                entry = self.gw.registry.get(sid)
                if entry and entry.info.controllable:
                    self.gw.controller.refresh_state(entry)
        response = pb.GatewayResponse(success=True)
        response.sources.sources.extend(self.gw.registry.summaries())
        return response

    def _command(self, cmd: pb.ControlCommand) -> pb.GatewayResponse:
        entry = self.gw.registry.get(cmd.source_id)
        if entry is None:
            return pb.GatewayResponse(success=False, error=f"fonte '{cmd.source_id}' nao encontrada")
        if cmd.WhichOneof("action") is None:
            return pb.GatewayResponse(success=False, error="comando sem acao")
        reply = self.gw.controller.execute(entry, cmd)
        response = pb.GatewayResponse(success=reply.success, error="" if reply.success else reply.message)
        response.command_reply.CopyFrom(reply)
        return response

    def _query(self, query: pb.QueryRequest) -> pb.GatewayResponse:
        result = run_query(self.gw.store, self.gw.registry, query)
        response = pb.GatewayResponse(success=True)
        response.query.CopyFrom(result)
        return response


class ClientServer:
    def __init__(self, port: int, gateway):
        self.gw = gateway
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(5)
        self.sock.settimeout(1.0)
        self.port = self.sock.getsockname()[1]

    def serve_forever(self, stop_event: threading.Event) -> None:
        log.info("servidor TCP para clientes escutando na porta %d", self.port)
        while not stop_event.is_set():
            try:
                conn, addr = self.sock.accept()
            except socket.timeout:
                continue
            conn.settimeout(None)
            ClientHandler(conn, addr, self.gw).start()
        self.sock.close()
