# Trabalho 1 - Distribuicao de Processos e Dados

**Dominio:** Data Center (DCIM - Data Center Infrastructure Management)

## Estrutura do Projeto

- `protos/datacenter.proto`: Definicao das mensagens Protocol Buffers e servicos gRPC (`datacenter_pb2.py` e `datacenter_pb2_grpc.py`).
- `common/`: Configuracoes de portas/enderecos, sockets multicast e enquadramento TCP.
- `sources/`:
  - `crac.py`: Fonte A (Controlavel via gRPC + fluxo UDP) - Ar-condicionado de precisao.
  - `rack_sensor.py`: Fonte B (Controlavel via gRPC + fluxo UDP + evento imediato de limiar) - Sensor de temperatura e umidade do rack.
  - `pdu.py`: Fonte C (Nao controlavel, fluxo continuo UDP) - Unidade de distribuicao de energia (PDU).
- `gateway/`: Processo central (descoberta multicast, servidor UDP de coleta, cliente gRPC, armazenamento em memoria/CSV, consultas agregadas e servidor TCP).
- `client/client.py`: Cliente com interface grafica Web conectado ao Gateway via TCP.

## Instalacao

```powershell
pip install -r requirements.txt
```

## Execucao

```powershell
python -m client.client
```

Acesse `http://127.0.0.1:8080`.
