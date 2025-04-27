import psycopg2
from pymongo import MongoClient
import time
import argparse
import statistics
import os
import sys
import resource
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_config import PG_PARAMS, get_mongo_uri, DEFAULT_DB_NAME

from queries.benchmark_queries import QUERIES, list_queries as list_available_queries

def init_connections():
    """Initialize connections to both databases"""
    pg_conn = psycopg2.connect(**PG_PARAMS)
    mongo_client = MongoClient(get_mongo_uri())
    mongo_db = mongo_client[DEFAULT_DB_NAME]
    return pg_conn, mongo_db, mongo_client

def close_connections(pg_conn, mongo_client):
    """Close database connections"""
    if pg_conn:
        pg_conn.close()
    if mongo_client:
        mongo_client.close()

def get_memory_usage():
    """Get current memory usage in MB"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

def run_postgres_query(conn, query, params=None, iterations=3):
    """Run a PostgreSQL query and measure execution time"""
    cursor = conn.cursor()
    times = []
    rows = None
    
    # Warm-up run
    cursor.execute(query, params or [])
    rows = cursor.fetchall()
    
    # Timed runs
    for i in range(iterations):
        start_mem = get_memory_usage()
        start_time = time.time()
        
        cursor.execute(query, params or [])
        rows = cursor.fetchall()
        
        end_time = time.time()
        end_mem = get_memory_usage()
        
        times.append(end_time - start_time)
        
    cursor.close()
    return {
        'times': times,
        'row_count': len(rows) if rows else 0,
        'memory_delta': end_mem - start_mem
    }

def run_mongo_query(db, query_func, iterations=3):
    """Run a MongoDB query and measure execution time"""
    times = []
    result = None
    
    # Warm-up run
    result = query_func(db)
    
    # Timed runs
    for i in range(iterations):
        start_mem = get_memory_usage()
        start_time = time.time()
        
        result = query_func(db)
        
        end_time = time.time()
        end_mem = get_memory_usage()
        
        times.append(end_time - start_time)
        
    return {
        'times': times,
        'row_count': len(result) if result else 0,
        'memory_delta': end_mem - start_mem
    }

def run_benchmark(query_name, pg_conn, mongo_db, iterations=3):
    """Run benchmark for a specific query on both databases"""
    if query_name not in QUERIES:
        print(f"Query '{query_name}' not found in predefined queries")
        return None
    
    query_info = QUERIES[query_name]
    print(f"\nRunning benchmark: {query_info['description']}")
    
    print("  Running PostgreSQL query...")
    pg_result = run_postgres_query(
        pg_conn, 
        query_info['pg'], 
        query_info.get('pg_params', []), 
        iterations
    )
    
    print("  Running MongoDB query...")
    mongo_result = run_mongo_query(
        mongo_db, 
        query_info['mongo'], 
        iterations
    )
    
    return {
        'query_name': query_name,
        'description': query_info['description'],
        'postgresql': {
            'avg_time': statistics.mean(pg_result['times']),
            'min_time': min(pg_result['times']),
            'max_time': max(pg_result['times']),
            'row_count': pg_result['row_count'],
            'memory_delta': pg_result['memory_delta']
        },
        'mongodb': {
            'avg_time': statistics.mean(mongo_result['times']),
            'min_time': min(mongo_result['times']),
            'max_time': max(mongo_result['times']),
            'row_count': mongo_result['row_count'],
            'memory_delta': mongo_result['memory_delta']
        }
    }

def print_results(results):
    """Print benchmark results in a tabular format"""
    table_data = []
    
    for result in results:
        pg_data = result['postgresql']
        mongo_data = result['mongodb']
        
        if mongo_data['avg_time'] > 0 and pg_data['avg_time'] > 0:
            if mongo_data['avg_time'] > pg_data['avg_time']:
                comparison = f"PostgreSQL {mongo_data['avg_time']/pg_data['avg_time']:.2f}x faster"
            else:
                comparison = f"MongoDB {pg_data['avg_time']/mongo_data['avg_time']:.2f}x faster"
        else:
            comparison = "N/A"
        
        table_data.append([
            result['query_name'],
            f"{pg_data['avg_time']*1000:.2f}ms",
            f"{mongo_data['avg_time']*1000:.2f}ms",
            comparison,
            pg_data['row_count'],
            mongo_data['row_count'],
            f"{pg_data['memory_delta']:.2f}MB",
            f"{mongo_data['memory_delta']:.2f}MB"
        ])
    
    headers = ["Query", "PG Avg Time", "Mongo Avg Time", "Comparison", 
               "PG Rows", "Mongo Rows", "PG Mem Δ", "Mongo Mem Δ"]
    
    print("\n=== Benchmark Results ===")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def save_results_to_csv(results, filename="benchmark_results.csv"):
    """Save benchmark results to a CSV file"""
    data = []
    
    for result in results:
        pg_data = result['postgresql']
        mongo_data = result['mongodb']
        
        data.append({
            'Query': result['query_name'],
            'Description': result['description'],
            'PG Avg Time (ms)': pg_data['avg_time'] * 1000,
            'PG Min Time (ms)': pg_data['min_time'] * 1000,
            'PG Max Time (ms)': pg_data['max_time'] * 1000,
            'PG Rows': pg_data['row_count'],
            'PG Memory Delta (MB)': pg_data['memory_delta'],
            'Mongo Avg Time (ms)': mongo_data['avg_time'] * 1000,
            'Mongo Min Time (ms)': mongo_data['min_time'] * 1000,
            'Mongo Max Time (ms)': mongo_data['max_time'] * 1000,
            'Mongo Rows': mongo_data['row_count'],
            'Mongo Memory Delta (MB)': mongo_data['memory_delta']
        })
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"\nResults saved to {filename}")

def generate_charts(results, output_dir="benchmark_charts"):
    """Generate charts comparing PostgreSQL and MongoDB performance"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    query_names = [r['query_name'] for r in results]
    pg_times = [r['postgresql']['avg_time'] * 1000 for r in results]
    mongo_times = [r['mongodb']['avg_time'] * 1000 for r in results]
    
    plt.figure(figsize=(12, 6))
    bar_width = 0.35
    x = range(len(query_names))
    
    plt.bar([i - bar_width/2 for i in x], pg_times, bar_width, label='PostgreSQL')
    plt.bar([i + bar_width/2 for i in x], mongo_times, bar_width, label='MongoDB')
    
    plt.xlabel('Query')
    plt.ylabel('Average Execution Time (ms)')
    plt.title('Database Performance Comparison')
    plt.xticks(x, query_names, rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()
    
    plt.savefig(os.path.join(output_dir, 'execution_times.png'))
    print(f"Chart saved to {os.path.join(output_dir, 'execution_times.png')}")

def main():
    parser = argparse.ArgumentParser(description='Benchmark PostgreSQL vs MongoDB for Yelp dataset')
    parser.add_argument('--queries', nargs='+', help='Specific queries to run (default: all)')
    parser.add_argument('--list', action='store_true', help='List available queries')
    parser.add_argument('--iterations', type=int, default=3, help='Number of iterations for each query (default: 3)')
    parser.add_argument('--csv', type=str, help='Save results to CSV file')
    parser.add_argument('--charts', action='store_true', help='Generate charts comparing performance')
    
    args = parser.parse_args()
    
    if args.list:
        list_available_queries()
        return
    
    pg_conn, mongo_db, mongo_client = init_connections()
    
    try:
        query_names = args.queries if args.queries else list(QUERIES.keys())
        
        results = []
        for query_name in query_names:
            if query_name in QUERIES:
                result = run_benchmark(query_name, pg_conn, mongo_db, args.iterations)
                if result:
                    results.append(result)
            else:
                print(f"Warning: Query '{query_name}' not found, skipping")
        
        if results:
            print_results(results)
            
            if args.csv:
                save_results_to_csv(results, args.csv)
            
            if args.charts:
                generate_charts(results)
        else:
            print("No benchmark results to report")
            
    finally:
        close_connections(pg_conn, mongo_client)

if __name__ == "__main__":
    main() 