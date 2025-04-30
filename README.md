# Data 101 Final Project: Database Systems Comparison

## Overview

Please read the full spec on the [Data 101 Website](https://data101.org/fa24/assignments/final-project/)

---

## Repository Structure

```
.
├── README.md                   
├── code/                       # Scripts for loading data and running benchmarks
│   ├── benchmark.py            # Main benchmarking script
│   ├── db_config.py            # Database connection configuration
│   ├── reset_load_mongo.py     # Script to load data into MongoDB
│   └── reset_load_postgres.py  # Script to load data into PostgreSQL
├── queries/                    # SQL and MongoDB queries
│   ├── benchmark_queries.py    # Query definitions for benchmarking
│   └── schema.sql              # PostgreSQL schema definition
├── docker-compose.yml          
├── Dockerfile                  
└── data/                       # Data directory (excluded from git)
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- At least 16GB RAM recommended (Docker memory allocation)
- At least 20GB free disk space

### Step 1: Clone the Repository

```bash
git clone https://github.com/arnursabet/yelp-db-benchmarks.git
cd yelp-db-benchmarks
```

### Step 2: Download the Yelp Dataset

1. Download the Yelp dataset from [Yelp Dataset](https://www.yelp.com/dataset) (JSON format)
2. Create a data directory and extract the files:

```bash
mkdir -p data/yelp_dataset
tar -xf path/to/yelp_dataset.tar -C data/yelp_dataset
```

3. Verify the extracted files:

```bash
ls -la data/yelp_dataset
```

You should see files like `yelp_academic_dataset_business.json`, `yelp_academic_dataset_review.json`, etc.

### Step 3: Build and Start Docker Containers

```bash
docker-compose build
docker-compose up -d
```

Verify the containers are running:

```bash
docker-compose ps
```

You should see three containers: `yelp_postgres`, `yelp_mongodb`, and `yelp_python`.

### Step 4: Load Data into PostgreSQL

```bash
docker exec yelp_python python /app/code/reset_load_postgres.py
```

This process will take some time depending on your machine's specs. You can monitor progress:

```bash
docker logs -f yelp_python
```

### Step 5: Load Data into MongoDB

```bash
# Load all collections
docker exec yelp_python python /app/code/reset_load_mongo.py

# Selectively load specific collections
docker exec yelp_python python /app/code/reset_load_mongo.py --collections businesses users

# Skip validation for faster loading (but may include invalid references)
docker exec yelp_python python /app/code/reset_load_mongo.py --skip-validation
```

## Running Benchmarks

The benchmarking system compares query performance between PostgreSQL and MongoDB using predefined queries. The benchmark tool runs the same queries against both databases and measures execution time, rows returned, and other performance metrics.

### Basic Benchmark

```bash
docker exec yelp_python python /app/code/benchmark.py
```

### Available Options

- List available queries:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --list
  ```

- Run specific queries:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --queries dancing_restaurants_philly
  ```

- Save results to a specific directory:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --results-dir /app/my_results
  ```
  
- Disable timestamps in filenames (will overwrite previous results):
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --no-timestamp
  ```

### Understanding Benchmark Results

When you run a benchmark, you'll see a summary table in the console:

```
=== Benchmark Results Summary ===
+----------------------------+---------------------+---------+------------------------+-----------+--------------------+
| Query                      | PG Execution Time   | PG Rows | Mongo Execution Time   | Mongo Rows| Mongo Docs Examined|
+============================+=====================+=========+========================+===========+====================+
| dancing_restaurants_philly | 79.45ms             | 10      | 83ms                   | 10        | 14569              |
+----------------------------+---------------------+---------+------------------------+-----------+--------------------+
```

This shows:
- Execution time for each database
- Number of rows/documents returned
- Number of documents examined by MongoDB

The benchmark also saves detailed explain results to JSON files in the `code/results` directory with timestamps:
- `postgres_explain_results_20231015_120530.json` 
- `mongo_explain_results_20231015_120530.json`

Latest results are also saved as:
- `latest_postgres_explain_results.json`
- `latest_mongo_explain_results.json`

These files contain detailed execution plans that can be analyzed to understand query performance.

## Adding New Benchmark Queries

To add new queries for benchmarking, follow these steps:

### Step 1: Edit the Benchmark Queries File

Edit `queries/benchmark_queries.py` and add a new entry to the `QUERIES` dictionary. Your query should include:

```python
'your_query_name': {
    'description': 'Brief description of what the query does',
    
    # PostgreSQL Query
    'pg': """
        SELECT column1, column2
        FROM your_table
        WHERE condition = %s
        ORDER BY column1
        LIMIT 10
    """,
    'pg_params': ['parameter_value'],  # Parameters for the PostgreSQL query
    
    # MongoDB Query (standard version)
    'mongo': lambda db: list(db.collection.aggregate([
        {'$match': {'field': 'value'}},
        {'$sort': {'sort_field': -1}},
        {'$limit': 10}
    ])),
    
    # MongoDB Query (explain version)
    'mongo_explain': lambda db: db.command(
        'explain',
        {
            'aggregate': 'collection_name',
            'pipeline': [
                {'$match': {'field': 'value'}},
                {'$sort': {'sort_field': -1}},
                {'$limit': 10}
            ],
            'cursor': {}
        },
        verbosity='executionStats'
    )
}
```

### Step 2: Understanding Query Structure

#### PostgreSQL Queries:
- Use `%s` placeholders for parameters
- Include parameter values in the `pg_params` list
- Write clear, optimized SQL with proper formatting

#### MongoDB Queries:
- The `mongo` function runs the actual query
- The `mongo_explain` function gets the execution plan
- Parameters should match between both functions
- Make use of appropriate operators and index hints

### Step 3: Run Your Query

Test your query with:

```bash
docker exec yelp_python python /app/code/benchmark.py --queries your_query_name
```

### Step 4: Analyze the Results

1. Check the summary output in the console
2. Examine the JSON files in the `code/results` directory
3. Look for:
   - Execution time differences
   - Number of rows examined vs. returned
   - Index usage (or lack thereof)
   - Join strategies in PostgreSQL
   - Pipeline stages in MongoDB

## Useful Commands

- Access PostgreSQL CLI:
  ```bash
  docker exec -it yelp_postgres psql -U postgres -d yelp_db
  ```

- Access MongoDB shell:
  ```bash
  docker exec -it yelp_mongodb mongosh --username mongodb --password mongodb
  ```

- Check container logs:
  ```bash
  docker logs -f yelp_postgres
  docker logs -f yelp_mongodb
  docker logs -f yelp_python
  ```

- Restart containers:
  ```bash
  docker-compose restart
  ```

- Stop and remove containers:
  ```bash
  docker-compose down
  ```
