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

The benchmarking system compares query performance between PostgreSQL and MongoDB using predefined queries.

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
  docker exec yelp_python python /app/code/benchmark.py --queries business_by_city business_by_stars
  ```

- Change the number of iterations:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --iterations 5
  ```

- Save results to CSV:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --csv results.csv
  ```

- Generate performance charts:
  ```bash
  docker exec yelp_python python /app/code/benchmark.py --charts
  ```

## Adding New Benchmark Queries

To add new queries for benchmarking, edit the `queries/benchmark_queries.py` file.

1. Add a new entry to the `QUERIES` dictionary following this format:

```python
'query_name': {
    'description': 'Brief description of what the query does',
    'pg': 'SQL query for PostgreSQL with %s placeholders for parameters',
    'pg_params': [param1, param2, ...],  # Parameters for the PostgreSQL query
    'mongo': lambda db: list(db.collection.find({
        # MongoDB query criteria
    }).limit(100))
}
```

2. Run the benchmark with your new query:

```bash
docker exec yelp_python python /app/code/benchmark.py --queries your_query_name
```

### Example Query Format

```python
'business_by_city': {
    'description': 'Find businesses in a specific city',
    'pg': 'SELECT business_id, name, stars, review_count FROM businesses WHERE city = %s LIMIT 100',
    'pg_params': ['Berkeley'],
    'mongo': lambda db: list(db.businesses.find(
        {'city': 'Berkeley'}, 
        {'_id': 1, 'name': 1, 'stars': 1, 'review_count': 1}
    ).limit(100))
}
```

## Troubleshooting

- **Docker memory issues**: Increase memory allocation in Docker settings
- **Slow data loading**: For MongoDB, try using `--skip-validation` option
- **Database connection errors**: Check if containers are running with `docker-compose ps`
- **MongoDB duplicate key errors**: The script handles duplicates, but if issues persist, check data integrity

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
