# Ethereum ETL for PostgreSQL

The steps below will allow you to bootstrap a PostgreSQL database in GCP with full historical and real-time Ethereum
data:
blocks, transactions, logs, token_transfers, and traces.

The whole process will take between 24 and 72 hours.

**Prerequisites**:

- Python 3.6+
- gcloud
- psql

### 1. Get CSV files from ethereum-etl


### 2. Import data from CSV files to PostgreSQL database 


- Create the database and the tables:

```bash
cat schema/*.sql | psql -U postgres -d ethereum -h 127.0.0.1  --port 5432 -a
```

- Run import from local csv to local SQL:

create .env file with the following content:

```text
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_HOST=
```

run the following command:

```bash
python local_compose.py
```

Importing to  SQL is going to take between 12 and 24 hours.

### 3. Apply indexes to the tables

NOTE: indexes won't work for the contracts table due to the issue described
here https://github.com/blockchain-etl/ethereum-etl-postgres/pull/11#issuecomment-1107801061

- Run:

```bash
cat indexes/*.sql | psql -U postgres -d ethereum -h 127.0.0.1  --port 5433 -a
```

Creating indexes is going to take between 12 and 24 hours. Depending on the queries you're going to run
you may need to create more indexes or [partition](https://www.postgresql.org/docs/11/ddl-partitioning.html) the tables.

Cloud SQL instance will cost you between $200 and $500 per month depending on
whether you use HDD or SSD and on the machine type.

### 4. Streaming

Use `ethereumetl stream` command to continually pull data from an Ethereum node and insert it to Postgres tables:
https://github.com/blockchain-etl/ethereum-etl/tree/develop/docs/commands.md#stream.

Follow the instructions here to deploy it to Kubernetes: https://github.com/blockchain-etl/blockchain-etl-streaming.