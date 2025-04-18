# Data 101 Final Project: Database Systems Comparison

## Overview

Please read the full spec on the [Data 101 Website](https://data101.org/fa24/assignments/final-project/)

---

## Repository Structure

The final project contains this starter setup. You may adapt it as needed, but please make sure you don't commit large files which can be complicated to undo.

```
.
├── README.md           # Project documentation
├── 0-checkpoint.md     # Outline of the checkpoint submission
├── 1-final-report.md   # Outline for your final report
├── code/               # Place to put code, scripts, etc.
├── data/               # Data directory (excluded from git)
├── queries/            # Place to put SQL queries
```

**Warning:** It can be tricky to work with large files and git / GitHub. We've set up a `.gitignore` to exclude many common large files, and **everything** inside `data/`.

---

## Installation Guide for macOS

### Step 1: Install Docker Desktop

1. Download Docker Desktop for Mac from the [official Docker website](https://www.docker.com/products/docker-desktop/)
2. Open the downloaded `.dmg` file and drag Docker to your Applications folder
3. Open Docker from your Applications folder
4. During the first run, you might need to authorize the installation with your system password
5. Wait for Docker to start (the whale icon in the menu bar will stop animating when it's ready)
6. You may need to increase the memory allocation for Docker:
   - Click on the Docker icon in the menu bar
   - Select "Preferences" or "Settings"
   - Go to "Resources"
   - Set memory to at least 8GB (more if available)
   - Click "Apply & Restart"

### Step 2: Clone the Repository

1. Open Terminal
2. Clone the repository:
   ```bash
   git clone https://github.com/arnursabet/yelp-db-benchmarks.git
   cd yelp-db-benchmarks
   ```

### Step 3: Download the Yelp Dataset

1. Go to [Yelp Dataset](https://www.yelp.com/dataset) and accept the terms to download the dataset
2. Download the JSON version (around 5GB compressed)
3. Create a folder for the dataset:
   ```bash
   mkdir -p data/yelp_dataset
   ```
4. Move the downloaded file (typically named `yelp_dataset.tar`) to the data directory
5. Extract the files:
   ```bash
   tar -xf data/yelp_dataset.tar -C data/yelp_dataset
   ```
6. Verify the extracted files:
   ```bash
   ls -la data/yelp_dataset
   ```
   You should see files like `yelp_academic_dataset_business.json`, `yelp_academic_dataset_review.json`, etc.

### Step 4: Build and Start the Docker Containers

1. Make sure Docker Desktop is running
2. Build the Docker containers:
   ```bash
   docker-compose build
   ```
3. Start the containers:
   ```bash
   docker-compose up -d
   ```
4. Verify the containers are running:
   ```bash
   docker-compose ps
   ```
   You should see three containers running: `yelp_postgres`, `yelp_mongodb`, and `yelp_python`

### Step 5: Load the Data into PostgreSQL

1. Run the data loading script:
   ```bash
   docker exec yelp_python python /app/code/reset_load.py
   ```
   This process will take some time (possibly hours) depending on your machine's specs.

2. You can monitor the progress by checking the logs:
   ```bash
   docker logs -f yelp_python
   ```

### Step 6: Test the PostgreSQL Installation

1. Run a test query to verify the data was loaded correctly:
   ```bash
   docker exec yelp_postgres psql -U postgres -d yelp_db -f /queries/test_query.sql
   ```

2. You can also connect to the PostgreSQL database directly:
   ```bash
   docker exec -it yelp_postgres psql -U postgres -d yelp_db
   ```
   Then run SQL queries directly, for example:
   ```sql
   SELECT COUNT(*) FROM businesses;
   ```

### Step 7: Setting Up MongoDB (Coming Soon)

We're still working on the MongoDB setup and benchmarking. This section will be updated soon with instructions for loading data into MongoDB and running benchmark queries.

### Troubleshooting

- **Docker permission issues**: Make sure Docker Desktop is running before executing commands
- **Out of disk space**: The Yelp dataset is large, ensure you have enough free space (at least 20GB)
- **Memory errors**: Increase the memory allocation for Docker in the Docker Desktop settings
- **Connection errors**: Ensure the containers are running with `docker-compose ps`
- **Slow data loading**: This is expected, especially on machines with limited resources

### Useful Commands

- Stop the containers: `docker-compose down`
- Restart the containers: `docker-compose restart`
- View container logs: `docker logs -f yelp_postgres`
- Execute a command in a container: `docker exec yelp_postgres [command]`
- Open a shell in a container: `docker exec -it yelp_postgres bash`
