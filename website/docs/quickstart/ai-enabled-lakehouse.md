---
title: Vector Search with Lance
sidebar_position: 4
---

# Vector Search with Lance

This guide demonstrates how to build a vector search pipeline using Fluss with Lance. You will:
1. Use Flink 2.2's `CREATE MODEL` and `ML_PREDICT` to generate vector embeddings via a local Ollama model
2. Store product catalog data with embeddings in Fluss
3. Tier the data to Lance format on S3
4. Perform native vector similarity search using Python
5. Build an ANN index for faster search at scale

The dataset includes both clean product descriptions and typo variants (with a random letter dropped) to demonstrate that semantic embeddings are robust to imperfect text.

## Environment Setup

### Prerequisites

Before proceeding with this guide, ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.
All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

You will also need [Python 3.9+](https://www.python.org/downloads/) installed for the vector search step.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/) (however, Compose v1 might work with a few adaptions).
:::

### Starting required components

We will use `docker compose` to spin up the required components for this tutorial.

1. Create a working directory for this guide.

```shell
mkdir fluss-quickstart-lance
cd fluss-quickstart-lance
```

2. Create directories and download required jars:

```shell
mkdir -p lib opt vector-search

# Fluss Flink connector (Flink 2.2)
curl -fL -o "lib/fluss-flink-2.2-$FLUSS_VERSION$.jar" "$FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-2.2/$FLUSS_VERSION$/fluss-flink-2.2-$FLUSS_VERSION$.jar"

# Fluss Lance lake plugin
curl -fL -o "lib/fluss-lake-lance-$FLUSS_VERSION$.jar" "$FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-lance/$FLUSS_VERSION$/fluss-lake-lance-$FLUSS_VERSION$.jar"

# Flink OpenAI model provider (for ML_PREDICT with Ollama)
curl -fL -o lib/flink-model-openai-2.2.0.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-model-openai/2.2.0/flink-model-openai-2.2.0.jar

# Tiering service
curl -fL -o "opt/fluss-flink-tiering-$FLUSS_VERSION$.jar" "$FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar"
```

3. Create a `docker-compose.yml` file with the following content:

```yaml
services:
  #begin RustFS (S3-compatible storage)
  rustfs:
    image: rustfs/rustfs:1.0.0-alpha.83
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - RUSTFS_ACCESS_KEY=rustfsadmin
      - RUSTFS_SECRET_KEY=rustfsadmin
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs-data:/data
    command: /data
  rustfs-init:
    image: minio/mc
    depends_on:
      - rustfs
    entrypoint: >
      /bin/sh -c "
      until mc alias set rustfs http://rustfs:9000 rustfsadmin rustfsadmin; do
        echo 'Waiting for RustFS...';
        sleep 1;
      done;
      mc mb --ignore-existing rustfs/fluss;
      "
  #end
  #begin Ollama (local embedding model)
  ollama:
    image: alpine/ollama:0.16.1
    ports:
      - "11434:11434"
    volumes:
      - ollama-data:/root/.ollama
    healthcheck:
      test: ["CMD", "ollama", "list"]
      interval: 10s
      timeout: 5s
      retries: 5
  ollama-init:
    image: curlimages/curl:8.18.0
    depends_on:
      ollama:
        condition: service_healthy
    restart: "no"
    entrypoint: >
      sh -c "curl -X POST http://ollama:11434/api/pull -d '{\"name\":\"all-minilm\"}' --max-time 300"
  #end
  #begin Fluss cluster
  coordinator-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      zookeeper:
        condition: service_started
      rustfs-init:
        condition: service_completed_successfully
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.path.style.access: true
        datalake.format: lance
        datalake.lance.warehouse: s3://fluss/lance
        datalake.lance.endpoint: http://rustfs:9000
        datalake.lance.allow_http: true
        datalake.lance.access_key_id: rustfsadmin
        datalake.lance.secret_access_key: rustfsadmin
    volumes:
      - ./lib/fluss-lake-lance-$FLUSS_VERSION$.jar:/opt/fluss/plugins/lance/fluss-lake-lance-$FLUSS_VERSION$.jar
  tablet-server:
    image: apache/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server:9123
        data.dir: /tmp/fluss/data
        remote.data.dir: s3://fluss/remote-data
        s3.endpoint: http://rustfs:9000
        s3.access-key: rustfsadmin
        s3.secret-key: rustfsadmin
        s3.path.style.access: true
        kv.snapshot.interval: 0s
        datalake.format: lance
        datalake.lance.warehouse: s3://fluss/lance
        datalake.lance.endpoint: http://rustfs:9000
        datalake.lance.allow_http: true
        datalake.lance.access_key_id: rustfsadmin
        datalake.lance.secret_access_key: rustfsadmin
    volumes:
      - ./lib/fluss-lake-lance-$FLUSS_VERSION$.jar:/opt/fluss/plugins/lance/fluss-lake-lance-$FLUSS_VERSION$.jar
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #end
  #begin Flink cluster (2.2)
  jobmanager:
    image: flink:2.2.0-java17
    ports:
      - "8083:8081"
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh jobmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt
  taskmanager:
    image: flink:2.2.0-java17
    depends_on:
      - jobmanager
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "cp /tmp/jars/*.jar /opt/flink/lib/ 2>/dev/null || true;
       cp /tmp/opt/*.jar /opt/flink/opt/ 2>/dev/null || true;
       /docker-entrypoint.sh taskmanager"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.task.off-heap.size: 512m
    volumes:
      - ./lib:/tmp/jars
      - ./opt:/tmp/opt
  #end

volumes:
  rustfs-data:
  ollama-data:
```

The Docker Compose environment consists of the following containers:
- **Fluss Cluster:** a Fluss `CoordinatorServer`, a Fluss `TabletServer` and a `ZooKeeper` server, configured with Lance as the datalake format.
- **Flink Cluster**: a Flink 2.2 `JobManager` and a Flink `TaskManager` container to execute queries. The TaskManager has increased off-heap memory (512m) as required by Lance's Arrow-based connector.
- **Ollama**: a local embedding model server running the `all-minilm` model (384-dimension sentence embeddings, ~45MB). An init container automatically pulls the model on startup.
- **RustFS**: an S3-compatible storage system used both as Fluss remote storage and Lance's warehouse.

:::tip
[RustFS](https://github.com/rustfs/rustfs) is used as replacement for S3 in this quickstart example, for your production setup you may want to configure this to use cloud file system. See [here](/maintenance/filesystems/overview.md) for information on how to setup cloud file systems.
:::

:::tip
Ollama runs on CPU by default, which is sufficient for the small `all-minilm` model. No GPU is required for this quickstart. You can swap the Ollama endpoint for any OpenAI-compatible API (e.g., OpenAI, DeepSeek, vLLM) by changing the `endpoint` and `api-key` in the `CREATE MODEL` statement.
:::

4. To start all containers, run:
```shell
docker compose up -d
```
This command automatically starts all the containers defined in the Docker Compose configuration in detached mode.

Run
```shell
docker compose ps
```
to check whether all containers are running properly.

You can also visit http://localhost:8083/ to see if Flink is running normally.

5. Wait for the Ollama model to finish downloading before proceeding:
```shell
docker compose logs -f ollama-init
```
You should see a success message when the `all-minilm` model has been pulled. Press `Ctrl+C` to stop following the logs.

:::note
All the following commands involving `docker compose` should be executed in the created working directory that contains the `docker-compose.yml` file.
:::

Congratulations, you are all set!

## Enter into SQL-Client

First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./bin/sql-client.sh
```

## Create Embedding Model

Ollama exposes an OpenAI-compatible API at `/v1/embeddings`, so Flink's built-in `flink-model-openai` provider works directly. The `api-key` is required by the provider but Ollama ignores it.

The model is created in Flink's `default_catalog` because the Fluss catalog does not support `CREATE MODEL`. We will reference the Fluss table using its fully-qualified name (`fluss_catalog.lance_demo.products`) when inserting data.

```sql title="Flink SQL"
CREATE MODEL embedding_model
INPUT (`input` STRING)
OUTPUT (`embedding` ARRAY<FLOAT>)
WITH (
    'provider' = 'openai',
    'endpoint' = 'http://ollama:11434/v1/embeddings',
    'model' = 'all-minilm',
    'api-key' = 'unused'
);
```

:::tip
This quickstart uses `all-minilm`, a lightweight 384-dimension embedding model (~45MB) that runs on CPU. It is chosen for fast setup, not search quality. For production use cases, consider larger models with higher-dimensional embeddings for better accuracy, such as `nomic-embed-text` (768d) or `mxbai-embed-large` (1024d) via Ollama, or cloud provider APIs like OpenAI's `text-embedding-3-large` (3072d). Since Flink's `CREATE MODEL` uses the OpenAI-compatible API format, switching providers only requires changing the `endpoint`, `model`, and `api-key` — along with updating the `embedding.arrow.fixed-size-list.size` table property to match the new dimension.
:::

## Create Fluss Tables

### Create Fluss Catalog

Use the following SQL to create a Fluss catalog and the product table:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS lance_demo;
USE lance_demo;
```

### Create Product Table

Lance only supports log tables (no PRIMARY KEY). The `` `variant` `` column distinguishes clean product descriptions from typo variants.

The `embedding.arrow.fixed-size-list.size` property tells Fluss to write the `embedding` column as Arrow's `FixedSizeList<Float32>(384)` instead of a variable-length `List<Float32>`. This enables Lance's native vector search (`nearest`) and ANN indexing on the column.

```sql title="Flink SQL"
CREATE TABLE products (
    product_id INT,
    product_text STRING,
    `variant` STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'embedding.arrow.fixed-size-list.size' = '384'
);
```

Now switch back to the `default_catalog` so that `ML_PREDICT` can find the embedding model:

```sql title="Flink SQL"
USE CATALOG default_catalog;
USE default_database;
```

## Streaming into Fluss

### Create Product Catalog Source

Create a temporary view with 30 product descriptions spanning 6 categories:

```sql title="Flink SQL"
CREATE TEMPORARY VIEW source_products AS
SELECT * FROM (VALUES
    -- Electronics
    (1, 'Wireless Bluetooth Headphones with Active Noise Cancellation and 30-Hour Battery Life'),
    (2, 'Ultra-Slim Laptop with 16GB RAM and 512GB SSD for Professional Use'),
    (3, 'Portable Bluetooth Speaker with Waterproof Design and Deep Bass'),
    (4, 'Smartwatch with Heart Rate Monitor and GPS Tracking'),
    (5, 'Noise-Cancelling Wireless Earbuds with Charging Case'),
    -- Clothing
    (6, 'Lightweight Running Shoes with Breathable Mesh Upper and Cushioned Sole'),
    (7, 'Waterproof Winter Jacket with Insulated Down Filling'),
    (8, 'Classic Leather Belt with Brushed Metal Buckle'),
    (9, 'Cotton Crew Neck T-Shirt in Assorted Colors'),
    (10, 'Stretch Yoga Pants with High Waist and Side Pockets'),
    -- Kitchen
    (11, 'Stainless Steel Chef Knife with Ergonomic Handle'),
    (12, 'Non-Stick Ceramic Frying Pan with Heat-Resistant Handle'),
    (13, 'Programmable Coffee Maker with Built-In Grinder and Timer'),
    (14, 'High-Speed Blender for Smoothies and Frozen Drinks'),
    (15, 'Cast Iron Dutch Oven for Slow Cooking and Braising'),
    -- Sports
    (16, 'Carbon Fiber Tennis Racket with Vibration Dampening System'),
    (17, 'Adjustable Dumbbell Set with Quick-Change Weight Plates'),
    (18, 'Insulated Water Bottle that Keeps Drinks Cold for 24 Hours'),
    (19, 'Foldable Yoga Mat with Non-Slip Surface and Carrying Strap'),
    (20, 'Cycling Helmet with LED Safety Light and Ventilation'),
    -- Books
    (21, 'Introduction to Machine Learning with Python and Scikit-Learn'),
    (22, 'The Complete Guide to Mediterranean Cooking'),
    (23, 'World History Atlas with Detailed Maps and Timelines'),
    (24, 'Beginner Guitar Lesson Book with Audio Companion'),
    (25, 'Science Fiction Novel Set in a Distant Galaxy'),
    -- Home
    (26, 'Memory Foam Pillow with Cooling Gel Layer for Side Sleepers'),
    (27, 'LED Desk Lamp with Adjustable Brightness and USB Charging Port'),
    (28, 'Cordless Stick Vacuum with HEPA Filter and Wall Mount'),
    (29, 'Scented Soy Candle with Natural Essential Oils'),
    (30, 'Bamboo Shoe Rack with Three Tiers and Modern Design')
) AS t(product_id, product_text);
```

### Generate Embeddings and Insert Clean Products

Use `ML_PREDICT` to generate vector embeddings for each product description and insert them into the Fluss table (referenced by its fully-qualified name):

```sql title="Flink SQL"
INSERT INTO fluss_catalog.lance_demo.products
SELECT product_id, product_text, 'clean' AS `variant`, embedding
FROM ML_PREDICT(
    TABLE source_products,
    MODEL embedding_model,
    DESCRIPTOR(product_text)
);
```

### Generate Typo Variants

To demonstrate that semantic embeddings are robust to typos, create variants of each product description with one letter dropped. A deterministic position based on `product_id` ensures each product gets a consistent, unique typo:

```sql title="Flink SQL"
CREATE TEMPORARY VIEW typo_products AS
SELECT
    product_id,
    CONCAT(
        SUBSTR(product_text, 1, MOD(product_id * 7, CHAR_LENGTH(product_text) - 1) + 1 - 1),
        SUBSTR(product_text, MOD(product_id * 7, CHAR_LENGTH(product_text) - 1) + 1 + 1)
    ) AS product_text
FROM source_products;
```

```sql title="Flink SQL"
INSERT INTO fluss_catalog.lance_demo.products
SELECT product_id, product_text, 'typo' AS `variant`, embedding
FROM ML_PREDICT(
    TABLE typo_products,
    MODEL embedding_model,
    DESCRIPTOR(product_text)
);
```

You now have 60 rows (30 clean + 30 typo) in Fluss, each with an embedding.

### Quitting SQL Client

You can now quit the Flink SQL Client:
```sql title="Flink SQL"
quit;
```

## Lakehouse Integration

### Start the Lakehouse Tiering Service

To tier data from Fluss to Lance, you need to start the `Lakehouse Tiering Service`.
Open a new terminal, navigate to the `fluss-quickstart-lance` directory, and execute the following command:

```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
    /opt/flink/opt/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format lance \
    --datalake.lance.warehouse s3://fluss/lance \
    --datalake.lance.endpoint http://rustfs:9000 \
    --datalake.lance.allow_http true \
    --datalake.lance.access_key_id rustfsadmin \
    --datalake.lance.secret_access_key rustfsadmin
```

You should see a Flink Job to tier data from Fluss to Lance running in the [Flink Web UI](http://localhost:8083/).

Wait for the configured `datalake.freshness` (~30s) to complete. You can visit http://localhost:9001/ and sign in with `rustfsadmin` / `rustfsadmin` to verify Lance files appear at `fluss/lance/lance_demo/products.lance/`.

## Vector Similarity Search with Python

Now that the product data with embeddings has been tiered to Lance, you can perform vector similarity search directly on the Lance dataset using Python.

### Create the Search Script

1. Create `vector-search/requirements.txt`:

```text title="vector-search/requirements.txt"
pylance>=0.20.0
numpy
pandas
requests
```

2. Create `vector-search/search.py`:

```python title="vector-search/search.py"
import sys

import lance
import numpy as np
import requests


def get_embedding(text, ollama_url="http://localhost:11434"):
    """Get embedding for a text query using Ollama."""
    response = requests.post(
        f"{ollama_url}/api/embed",
        json={"model": "all-minilm", "input": text},
    )
    response.raise_for_status()
    return np.array(response.json()["embeddings"][0], dtype=np.float32)


def search_products(query, top_k=10):
    """Search for similar products using Lance native vector search."""
    # Embed the search query
    query_vector = get_embedding(query)

    # Open the Lance dataset from S3 (via RustFS)
    storage_options = {
        "aws_access_key_id": "rustfsadmin",
        "aws_secret_access_key": "rustfsadmin",
        "aws_endpoint": "http://localhost:9000",
        "aws_region": "us-east-1",
        "allow_http": "true",
    }
    ds = lance.dataset(
        "s3://fluss/lance/lance_demo/products.lance",
        storage_options=storage_options,
    )

    # Use Lance's native nearest() for k-nearest-neighbor vector search
    tbl = ds.to_table(
        nearest={
            "column": "embedding",
            "q": query_vector,
            "k": top_k,
        }
    )
    df = tbl.to_pandas()

    # Print results
    print(f'\nSearch query: "{query}"')
    print(f"\nTop {top_k} similar products:")
    print(f"{'Rank':<6}{'Distance':<12}{'Variant':<9}{'Product'}")
    print("-" * 100)
    for rank, (_, row) in enumerate(df.iterrows(), 1):
        print(
            f"{rank:<6}{row['_distance']:<12.4f}{row['variant']:<9}{row['product_text']}"
        )


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "wireless headphones"
    search_products(query)
```

### Run the Search

```shell
cd vector-search
pip install -r requirements.txt
python search.py "wireless headphones long battery"
```

**Sample Output:**
```
Search query: "wireless headphones long battery"

Top 10 similar products:
Rank  Distance    Variant  Product
----------------------------------------------------------------------------------------------------
1     0.6934      clean    Wireless Bluetooth Headphones with Active Noise Cancellation and 30-Hour Battery Life
2     0.7711      typo     Wireles Bluetooth Headphones with Active Noise Cancellation and 30-Hour Battery Life
3     1.0529      clean    Noise-Cancelling Wireless Earbuds with Charging Case
4     1.0685      typo     Noise-Cancelling Wireless Earbuds wth Charging Case
5     1.2891      clean    Portable Bluetooth Speaker with Waterproof Design and Deep Bass
...
```

Notice how:
- The most relevant products appear at the top with the smallest distances
- Typo variants rank right next to their clean originals with nearly identical distances, demonstrating that semantic embeddings are robust to minor text corruption
- Unrelated products (e.g., kitchen knives, yoga mats) have much larger distances

Try different queries to explore the semantic search:
```shell
python search.py "comfortable shoes for exercise"
python search.py "kitchen tools for cooking"
python search.py "books about technology"
```

### Build a Vector Index for ANN Search

The `nearest()` search above performs brute-force k-nearest-neighbor scan, which works well for small datasets. For larger datasets, you can build an ANN (Approximate Nearest Neighbor) index directly on the tiered Lance dataset for significantly faster search.

Create `vector-search/build_index.py`:

```python title="vector-search/build_index.py"
import lance

storage_options = {
    "aws_access_key_id": "rustfsadmin",
    "aws_secret_access_key": "rustfsadmin",
    "aws_endpoint": "http://localhost:9000",
    "aws_region": "us-east-1",
    "allow_http": "true",
}
ds = lance.dataset(
    "s3://fluss/lance/lance_demo/products.lance",
    storage_options=storage_options,
)

# Build an IVF_HNSW_SQ index on the embedding column
ds.create_index(
    "embedding",
    index_type="IVF_HNSW_SQ",
    metric="L2",
    replace=True,
)

print("Index created!")
print("Indices:", ds.list_indices())
```

Run it:

```shell
python build_index.py
```

Once the index is built, the same `search.py` script automatically uses it — no code changes needed. Lance detects the index and routes `nearest()` queries through ANN search instead of brute-force scan:

```shell
python search.py "sharp"
python search.py "comfortable shoes for exercise"
python search.py "books about technology"
```

:::tip
For production datasets, `IVF_PQ` provides better compression and is a good default. `IVF_HNSW_SQ` works well for smaller datasets. See the [Lance vector search documentation](https://docs.lancedb.com/search/vector-search) for details on index types and tuning.
:::

:::note
When Fluss tiers new data, the index covers only the rows that existed when it was created. Newly tiered rows are still searched via brute-force and merged with indexed results automatically. Periodically rebuild the index to keep it up to date.
:::

## Clean up
Run the following to stop all containers.
```shell
docker compose down -v
```
