[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/wWsgWD6e)

# Exercicio 10 - Kafka + RESTful

Esta versao reaproveita o produto do Exercicio 09 e substitui a camada
de **cliente-servidor gRPC** por uma API **RESTful (HTTP/JSON)** sobre Flask.
O fluxo pub-sub via Kafka permanece igual.

## Arquitetura

```
(1) sensor_producer.py       (Kafka producer)
        |
        v   topic: temperature_readings
(2) stats_processor.py       (Kafka consumer + producer)
        |
        v   topic: temperature_stats
(3) temperature_service.py   (Kafka consumer + REST API Flask + SQLite)
        ^
        |   HTTP/JSON
(4) temperature_client.py    (HTTP client - requests)
```

### Cenario

- (1) **Sensor simulado** gera leituras de temperatura. Publica em
  `temperature_readings` *somente* quando a variacao em relacao a ultima
  leitura enviada e significativa (limiar configuravel, padrao 0.5 C).
- (2) **Processador** consome `temperature_readings`, mantem por sensor uma
  janela deslizante de 2 horas e publica em `temperature_stats` a media
  movel (alem de min/max e contagem).
- (3) **Servico** consome `temperature_stats`, persiste em SQLite e expoe
  endpoints HTTP REST para consulta.
- (4) **Cliente** consome a API REST via `requests` para obter a ultima
  estatistica, estatistica por sensor, historico, lista de sensores e
  contagem total.

## Endpoints REST

| Metodo | Rota                                       | Descricao                                  |
| ------ | ------------------------------------------ | ------------------------------------------ |
| GET    | `/stats/latest`                            | Ultima estatistica registrada              |
| GET    | `/sensors/{sensor_id}/stats/latest`        | Ultima estatistica de um sensor especifico |
| GET    | `/sensors/{sensor_id}/history?limit=N`     | Historico do sensor (default 50)           |
| GET    | `/sensors`                                 | Lista de IDs de sensores conhecidos        |
| GET    | `/stats/count`                             | Quantidade total de estatisticas           |

Respostas em JSON. Codigos comuns: `200 OK`, `404 Not Found`, `400 Bad Request`.

### Exemplos

```
curl http://127.0.0.1:5000/sensors
curl http://127.0.0.1:5000/stats/latest
curl http://127.0.0.1:5000/sensors/sensor-A/stats/latest
curl "http://127.0.0.1:5000/sensors/sensor-A/history?limit=10"
curl http://127.0.0.1:5000/stats/count
```

## Como executar

### 0) Pre-requisitos

- Broker Kafka acessivel (ver instrucoes em `ex7`).
- Python 3 com as dependencias:

```
sudo apt update
sudo apt install python3-pip python3-venv
python3 -m venv myvenv
source myvenv/bin/activate
pip3 install kafka-python flask requests
```

### 1) Configurar enderecos

- `const.py` (raiz) e `python/const.py`: ajustar `BROKER_ADDR` para o IP do
  broker Kafka.
- `python/const.py`: ajustar `CLIENT_IP` para o IP onde o servico REST
  estara rodando, e `REST_HOST`/`REST_PORT` no servidor (use `0.0.0.0`
  para aceitar conexoes externas).

### 2) Subir o servico (consumidor + REST API)

Em um terminal (na pasta `python/`):

```
python3 temperature_service.py
```

Ele cria/usa `temperature_stats.db` (SQLite) no diretorio corrente.

### 3) Subir o processador

Em outro terminal (na raiz do projeto):

```
python3 stats_processor.py
```

### 4) Subir um (ou mais) sensor

Em outro(s) terminal(is) (na raiz do projeto):

```
python3 sensor_producer.py sensor-A
python3 sensor_producer.py sensor-B 0.3 0.5
```

Argumentos: `<sensor_id> [threshold C] [intervalo s]`.

### 5) Consultar via cliente REST

Em outro terminal (na pasta `python/`):

```
python3 temperature_client.py            # consulta geral
python3 temperature_client.py sensor-A 10  # historico do sensor-A (ate 10 entradas)
```

Tambem e possivel consultar diretamente com `curl` ou pelo navegador.

## Diferencas em relacao ao Exercicio 09 (gRPC)

- Removida a pasta `protos/` e a etapa de geracao de stubs com
  `grpc_tools.protoc`.
- O servidor agora usa **Flask** (HTTP/1.1 + JSON) em vez de `grpc.server`.
- O cliente usa **requests** em vez de stubs gRPC; nao ha contrato
  pre-compilado.
- A configuracao de porta passou de `GRPC_PORT` para `REST_PORT`
  (default 5000).
- Resposta padronizada em JSON, com codigos HTTP semanticos
  (`200`, `404`, `400`).
- Resto do pipeline (Kafka producer/consumer, SQLite, janela deslizante)
  permanece inalterado, demonstrando que a camada de exposicao do
  servico esta desacoplada da logica de dominio.

## Observacoes

- O sensor so publica quando ha variacao significativa - leituras
  estaveis nao geram trafego.
- A janela de 2 horas e calculada por sensor, com base nos timestamps
  recebidos pelo processador.
- A persistencia e feita em SQLite local para manter o exercicio simples.
