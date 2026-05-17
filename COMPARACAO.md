# Comparacao: gRPC (Ex9) vs RESTful (Ex10)

Os dois exercicios resolvem o mesmo problema (consulta de estatisticas
de temperatura agregadas por um pipeline Kafka), trocando apenas a
camada de exposicao do servico. Isso permite comparar gRPC e REST nos
mesmos casos de uso.

## 1. Criterios de desenvolvimento

### 1.1 Contrato de servico
- **gRPC (Ex9):** contrato formal em `.proto` (Protocol Buffers). E
  obrigatorio compilar os stubs antes de codar
  (`grpc_tools.protoc`). Isso da forte garantia de tipos e gera
  cliente/servidor automaticamente em varias linguagens, mas adiciona
  uma etapa de build e versionamento do schema.
- **REST (Ex10):** contrato implicito em URLs, metodos HTTP e payloads
  JSON. Nao requer geracao de codigo nem dependencia de IDL. E mais
  rapido para prototipar, porem fica sem validacao automatica do schema
  (a menos que se adote OpenAPI/Swagger a parte).

### 1.2 Modelagem da API
- **gRPC:** modelo orientado a chamadas de procedimento (`GetLatestStats`,
  `GetHistory`...). O design tende a parecer "metodos de classe
  remota".
- **REST:** modelo orientado a recursos (`/sensors`, `/sensors/{id}/history`).
  Casa bem com hierarquias de dados e e mais familiar para quem ja
  trabalhou com web.

### 1.3 Tipagem e serializacao
- **gRPC:** Protobuf, binario, tipado, com defaults bem definidos.
  Campos opcionais e renomeacoes precisam respeitar regras de
  compatibilidade.
- **REST/JSON:** texto, tipagem fraca (tudo string/numero), mas
  legivel a olho nu e facil de inspecionar em `curl`, navegador e
  ferramentas como Postman/Insomnia.

### 1.4 Ferramental e curva de aprendizado
- **gRPC:** exige conhecimento de Protobuf, geracao de stubs, plugins de
  IDE. Mais "infraestrutura conceitual" antes da primeira chamada.
- **REST:** funciona com `curl`, `requests`, `fetch`, qualquer navegador.
  Curva de entrada muito mais baixa, integrando facil com front-end web
  e dashboards.

### 1.5 Dependencias do projeto
- Ex9: `grpcio`, `grpcio-tools` (alem do Kafka).
- Ex10: apenas `flask` e `requests`. Menos coisas para instalar e
  manter.

## 2. Criterios de operacao

### 2.1 Desempenho e uso de rede
- **gRPC:** HTTP/2 + Protobuf binario. Payload menor, multiplexacao de
  streams, latencia tipicamente mais baixa. Vantagem clara em
  comunicacao maquina-a-maquina de alto volume.
- **REST/JSON:** HTTP/1.1, payload texto (maior). Sobrecarga maior por
  requisicao, mas suficiente para o volume do exercicio (consultas de
  monitoramento sob demanda).

### 2.2 Observabilidade e debug em producao
- **gRPC:** trafego binario; precisa de ferramentas especificas
  (`grpcurl`, BloomRPC) e proxies que entendam HTTP/2 para inspecao.
  Logs intermediarios sao menos uteis sem decoder.
- **REST:** trafego texto inspecionavel em qualquer proxy/IDS, logs de
  acesso padrao do Nginx/Apache, replay com `curl`. Diagnostico em
  ambiente distribuido tende a ser mais simples.

### 2.3 Infraestrutura, gateway e balanceadores
- **gRPC:** muitos load balancers L7 antigos so falam HTTP/1.1; e comum
  precisar de Envoy/Nginx recente ou Istio para balancear corretamente.
- **REST:** suportado por praticamente toda infraestrutura existente
  (CDNs, WAFs, gateways, proxies reversos), sem configuracao especial.

### 2.4 Cache
- **REST:** se beneficia diretamente da semantica HTTP (`Cache-Control`,
  `ETag`), util para consultas idempotentes como `/stats/latest` ou
  `/sensors`.
- **gRPC:** sem caching HTTP nativo; precisa ser implementado em camada
  de aplicacao.

### 2.5 Compatibilidade com clientes
- **gRPC:** ideal para clientes internos (services-to-service) e SDKs
  oficiais. Limitado em navegadores (depende de gRPC-Web + proxy).
- **REST:** clientes universais - navegador, mobile, scripts, planilhas,
  ferramentas no-code. Acessivel a equipes nao-backend.

### 2.6 Evolucao e versionamento
- **gRPC/Protobuf:** regras claras para adicionar campos sem quebrar
  clientes antigos, mas exige disciplina na manutencao do `.proto` e na
  redistribuicao dos stubs.
- **REST:** versionamento por URL (`/v1/...`), por header ou por campo.
  Mais flexivel e informal, com risco de divergencia se nao houver
  contrato (OpenAPI).

### 2.7 Seguranca
- Ambos podem rodar sobre TLS. Em REST, autenticacao por header
  (`Authorization`, JWT, OAuth2) e padrao da industria, com farta
  documentacao. Em gRPC, usa-se metadata + interceptors; e poderoso,
  mas com menos exemplos prontos.

## 3. Quando preferir cada um

- **gRPC** brilha em comunicacao interna entre microsservicos com
  contratos estaveis, alta carga, streaming bidirecional e equipes
  poliglotas (geracao automatica de clientes).
- **REST** brilha em APIs publicas/parcialmente publicas, integracoes
  heterogeneas, consumo por front-end web, diagnostico operacional
  rapido e prototipagem.

## 4. Conclusao no contexto deste exercicio

O servico exposto e tipico de consulta de monitoramento por humanos e
front-ends simples: poucas chamadas, payloads pequenos, necessidade de
inspecao rapida no terminal. Para esse cenario, o **REST/JSON do Ex10
oferece melhor relacao custo-beneficio**: menos dependencias, menos
etapas de build, melhor compatibilidade com ferramentas web e
diagnostico mais facil. O **gRPC do Ex9** continua sendo a melhor
escolha caso o consumidor seja outro servico interno com alto volume e
necessidade de contrato forte. A logica de dominio (Kafka + janela
deslizante + SQLite) e a mesma nos dois casos, evidenciando que a
escolha entre gRPC e REST e uma decisao de **interface**, nao de
arquitetura.
