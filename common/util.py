import logging

from protos import datacenter_pb2 as pb

TYPE_NAMES = {
    pb.PRECISION_AC: "Ar-condicionado de precisao",
    pb.RACK_SENSOR: "Sensor temp./umidade do rack",
    pb.PDU: "PDU (energia do rack)",
}

STATUS_NAMES = {
    pb.STATUS_UNKNOWN: "DESCONHECIDO",
    pb.ONLINE: "ONLINE",
    pb.STANDBY: "STANDBY",
    pb.FAULT: "FALHA",
    pb.OFFLINE: "OFFLINE",
}


def type_name(source_type: int) -> str:
    return TYPE_NAMES.get(source_type, "desconhecido")


def status_name(status: int) -> str:
    return STATUS_NAMES.get(status, "?")


def setup_logging(name: str, verbose: bool = False) -> logging.Logger:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(name)
