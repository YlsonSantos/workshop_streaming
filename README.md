# Workshop Data Streaming: Kafka to Apache Iceberg

Este projeto demonstra a implementação de um pipeline de dados em tempo real seguindo a **Arquitetura Medalhão** (Bronze, Silver e Gold). O fluxo consiste em capturar dados de aeroportos via API, processá-los através de mensagens no Kafka e transformá-los utilizando Spark e o formato de tabela Apache Iceberg.

## Arquitetura do Projeto

O pipeline está dividido nas seguintes etapas:

1.  **Ingestão**: Um Producer Python lê os dados e os envia em fragmentos para o tópico `airports_raw` no Kafka.
2.  **Camada Bronze**: Um Consumer processa os dados em micro-batches de 30 segundos e os persiste em formato brutos (.parquet).
3.  **Camada Silver**: Os dados são limpos, tipados e deduplicados usando Spark, sendo armazenados em tabelas **Apache Iceberg**.
4.  **Camada Gold**: Filtro refinado para extrair apenas aeroportos brasileiros para análise final.

## Tecnologias Utilizadas

* **Linguagem:** Python 3.13
* **Mensageria:** Apache Kafka & Zookeeper (via Docker)
* **Processamento:** Apache Spark 3.5.1
* **Armazenamento:** Apache Iceberg & Parquet
* **Ambiente:** Java 17 (Temurin)

## Pré-requisitos

* Docker Desktop
* Python 3.13+
* Java 17 (Configurado no PATH)
* Winutils.exe (para execução em ambiente Windows)

## Como Executar

### 1. Subir a Infraestrutura
```powershell
docker compose up -d
```

### 2. Iniciar o Streaming
```powershell
# Terminal 1: Receptor (Consumer)
python consumer_bronze.py

# Terminal 2: Emissor (Producer)
python producer.py
```

### 3. Processamento Analítico
```powershell
# Camada Silver (Tratamento)
python silver_iceberg.py

# Camada Gold (Negócio - Brasil)
python gold_brazil.py
```

### 📂 Estrutura do Projeto

```text
workshop_streaming/
├── .venv/                 # Ambiente virtual Python
├── data/                  # Armazenamento local de dados
│   ├── bronze/            # Camada Bronze (Arquivos Parquet brutos)
│   │   └── airports/
│   └── warehouse/         # Camada Silver e Gold (Tabelas Iceberg)
│       ├── silver/
│       └── gold/
├── .gitignore             # Arquivos ignorados pelo Git
├── consumer_bronze.py     # Script de consumo Kafka -> Bronze
├── docker-compose.yml     # Infraestrutura do Kafka (Docker)
├── gold_brazil.py         # Script de transformação Silver -> Gold
├── producer.py            # Script de envio API -> Kafka
├── README.md              # Documentação do projeto
├── requirements.txt       # Dependências do Python
└── silver_iceberg.py      # Script de transformação Bronze -> Silver
```