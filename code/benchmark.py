import psycopg2
from pymongo import MongoClient
import argparse
import os
import sys
from tabulate import tabulate
import json
import bson
from bson import json_util
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_config import PG_PARAMS, get_mongo_uri, DEFAULT_DB_NAME

from queries.benchmark_queries import QUERIES, list_queries as list_available_queries

class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bson.regex.Regex):
            return {"$regex": obj.pattern, "$options": obj.flags}
        try:
            return json_util.default(obj)
        except TypeError:
            return str(obj)
        return json.JSONEncoder.default(self, obj)

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

def run_postgres_explain(conn, query, params=None):
    """Run a PostgreSQL query with EXPLAIN ANALYZE and get JSON output"""
    cursor = conn.cursor()
    explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
    
    cursor.execute(explain_query, params or [])
    explain_results = cursor.fetchall()
    
    json_result = explain_results[0][0]
    
    cursor.close()
    return json_result

def run_benchmark(query_name, pg_conn, mongo_db):
    """Run benchmark for a specific query on both databases using EXPLAIN ANALYZE"""
    if query_name not in QUERIES:
        print(f"Query '{query_name}' not found in predefined queries")
        return None
    
    query_info = QUERIES[query_name]
    print(f"\nRunning benchmark: {query_info['description']}")
    
    print("  Running PostgreSQL EXPLAIN ANALYZE...")
    pg_explain = run_postgres_explain(
        pg_conn, 
        query_info['pg'], 
        query_info.get('pg_params', [])
    )
    
    print("  Running MongoDB explain()...")
    
    if 'mongo_explain' in query_info:
        mongo_explain = query_info['mongo_explain'](mongo_db)
    else:
        print(f"  Warning: No mongo_explain function for query '{query_name}'")
        mongo_explain = {"error": "No explain function defined for this query"}
    
    return {
        'query_name': query_name,
        'description': query_info['description'],
        'postgresql': pg_explain,
        'mongodb': mongo_explain
    }

def get_timestamp_str():
    """Get a timestamp string for filenames"""
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d_%H%M%S")

def save_results_to_json(results, pg_file="postgres_explain_results.json", mongo_file="mongo_explain_results.json"):
    """Save benchmark results to separate JSON files for PostgreSQL and MongoDB with timestamps"""
    pg_results = {}
    mongo_results = {}
    
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    timestamp = get_timestamp_str()
    
    pg_filename = f"{os.path.splitext(pg_file)[0]}_{timestamp}.json"
    mongo_filename = f"{os.path.splitext(mongo_file)[0]}_{timestamp}.json"
    
    pg_path = os.path.join(results_dir, pg_filename)
    mongo_path = os.path.join(results_dir, mongo_filename)
    
    for result in results:
        pg_results[result['query_name']] = {
            'description': result['description'],
            'explain_result': result['postgresql']
        }
        
        mongo_results[result['query_name']] = {
            'description': result['description'],
            'explain_result': result['mongodb']
        }
    
    with open(pg_path, 'w') as f:
        json.dump(pg_results, f, indent=2)
    print(f"\nPostgreSQL EXPLAIN results saved to {pg_path}")
    
    with open(mongo_path, 'w') as f:
        json.dump(mongo_results, f, indent=2, cls=MongoEncoder)
    print(f"MongoDB explain results saved to {mongo_path}")
    
    latest_pg_path = os.path.join(results_dir, "latest_" + os.path.basename(pg_file))
    latest_mongo_path = os.path.join(results_dir, "latest_" + os.path.basename(mongo_file))
    
    with open(latest_pg_path, 'w') as f:
        json.dump(pg_results, f, indent=2)
    
    with open(latest_mongo_path, 'w') as f:
        json.dump(mongo_results, f, indent=2, cls=MongoEncoder)
    print(f"Latest results also saved to {latest_pg_path} and {latest_mongo_path}")
    
    return results_dir, timestamp

def print_results_summary(results):
    """Print a simple summary of benchmark results"""
    table_data = []
    
    for result in results:
        pg_data = result['postgresql']
        pg_execution_time = "N/A"
        pg_rows_returned = "N/A"
        
        try:
            if isinstance(pg_data, list) and len(pg_data) > 0:
                if 'Execution Time' in pg_data[0]:
                    pg_execution_time = f"{pg_data[0]['Execution Time']:.2f}ms"
                
                if 'Plan' in pg_data[0]:
                    plan = pg_data[0]['Plan']
                    if 'Actual Rows' in plan:
                        pg_rows_returned = plan['Actual Rows']
        except (KeyError, TypeError, IndexError):
            pass
        
        mongo_data = result['mongodb']
        mongo_execution_time = "N/A"
        mongo_rows_returned = "N/A"
        mongo_docs_examined = "N/A"
        
        try:
            if 'executionStats' in mongo_data:
                stats = mongo_data['executionStats']
                if 'executionTimeMillis' in stats:
                    mongo_execution_time = f"{stats['executionTimeMillis']}ms"
                if 'nReturned' in stats:
                    mongo_rows_returned = stats['nReturned']
                if 'totalDocsExamined' in stats:
                    mongo_docs_examined = stats['totalDocsExamined']
            
            elif 'ok' in mongo_data and mongo_data.get('ok') == 1:
                if 'stages' in mongo_data:
                    final_stage = mongo_data['stages'][-1]
                    if 'nReturned' in final_stage:
                        mongo_rows_returned = final_stage['nReturned']
                
                if 'executionStats' in mongo_data:
                    stats = mongo_data['executionStats']
                    if 'executionTimeMillis' in stats:
                        mongo_execution_time = f"{stats['executionTimeMillis']}ms"
                    if 'nReturned' in stats:
                        mongo_rows_returned = stats['nReturned']
                    if 'totalDocsExamined' in stats:
                        mongo_docs_examined = stats['totalDocsExamined']
                
                if 'explainVersion' in mongo_data:
                    if 'stages' in mongo_data:
                        for stage in mongo_data['stages']:
                            if '$cursor' in stage and 'executionStats' in stage['$cursor']:
                                stats = stage['$cursor']['executionStats']
                                if 'executionTimeMillis' in stats:
                                    mongo_execution_time = f"{stats['executionTimeMillis']}ms"
                                if 'nReturned' in stats:
                                    mongo_rows_returned = stats['nReturned']
                                if 'totalDocsExamined' in stats:
                                    mongo_docs_examined = stats['totalDocsExamined']
                                break
                        if len(mongo_data['stages']) > 0:
                            final_stage = mongo_data['stages'][-1]
                            if 'nReturned' in final_stage:
                                mongo_rows_returned = final_stage['nReturned']
        except (KeyError, TypeError):
            pass
        
        table_data.append([
            result['query_name'],
            pg_execution_time,
            str(pg_rows_returned),
            mongo_execution_time,
            str(mongo_rows_returned),
            str(mongo_docs_examined)
        ])
    
    headers = ["Query", "PG Execution Time", "PG Rows", "Mongo Execution Time", "Mongo Rows", "Mongo Docs Examined"]
    
    print("\n=== Benchmark Results Summary ===")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    for result in results:
        query_name = result['query_name']
        pg_data = result['postgresql']
        mongo_data = result['mongodb']
        
        pg_rows = "N/A"
        pg_examined = "N/A"
        try:
            if isinstance(pg_data, list) and len(pg_data) > 0:
                if 'Plan' in pg_data[0]:
                    plan = pg_data[0]['Plan']
                    if 'Actual Rows' in plan:
                        pg_rows = plan['Actual Rows']
                    if 'Plans' in plan:
                        rows_examined = 0
                        scan_nodes = []
                        
                        def collect_scan_nodes(node):
                            if 'Node Type' in node and ('Scan' in node['Node Type'] or 'Seq Scan' in node['Node Type']):
                                scan_nodes.append(node)
                            if 'Plans' in node:
                                for child in node['Plans']:
                                    collect_scan_nodes(child)
                        
                        collect_scan_nodes(plan)
                        
                        for node in scan_nodes:
                            if 'Actual Rows' in node:
                                rows_examined += node['Actual Rows']
                        
                        if rows_examined > 0:
                            pg_examined = rows_examined
        except (KeyError, TypeError, IndexError):
            pass
        
        mongo_rows = "N/A"
        mongo_examined = "N/A"
        try:
            if 'stages' in mongo_data:
                final_stage = mongo_data['stages'][-1]
                if 'nReturned' in final_stage:
                    mongo_rows = final_stage['nReturned']
                
                for stage in mongo_data['stages']:
                    if '$cursor' in stage and 'executionStats' in stage['$cursor']:
                        stats = stage['$cursor']['executionStats']
                        if 'totalDocsExamined' in stats:
                            mongo_examined = stats['totalDocsExamined']
                        break
            elif 'executionStats' in mongo_data:
                stats = mongo_data['executionStats']
                if 'nReturned' in stats:
                    mongo_rows = stats['nReturned']
                if 'totalDocsExamined' in stats:
                    mongo_examined = stats['totalDocsExamined']
        except (KeyError, TypeError, IndexError):
            pass
            
        print(f"\nResults for {query_name}:")
        print(f"  PostgreSQL: {pg_rows} rows returned, {pg_examined} rows examined")
        print(f"  MongoDB: {mongo_rows} documents returned, {mongo_examined} documents examined")

def main():
    parser = argparse.ArgumentParser(description='Benchmark PostgreSQL vs MongoDB for Yelp dataset using EXPLAIN ANALYZE')
    parser.add_argument('--queries', nargs='+', help='Specific queries to run (default: all)')
    parser.add_argument('--list', action='store_true', help='List available queries')
    parser.add_argument('--pg-output', type=str, default='postgres_explain_results.json', help='Output file for PostgreSQL EXPLAIN results')
    parser.add_argument('--mongo-output', type=str, default='mongo_explain_results.json', help='Output file for MongoDB explain results')
    parser.add_argument('--results-dir', type=str, default=None, help='Directory to save results (default: ./results)')
    parser.add_argument('--no-timestamp', action='store_true', help='Disable timestamps in filenames')
    
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
                result = run_benchmark(query_name, pg_conn, mongo_db)
                if result:
                    results.append(result)
            else:
                print(f"Warning: Query '{query_name}' not found, skipping")
        
        if results:
            print_results_summary(results)
            
            results_dir = args.results_dir
            if results_dir is None:
                results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
            
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            
            if args.no_timestamp:
                pg_path = os.path.join(results_dir, args.pg_output)
                mongo_path = os.path.join(results_dir, args.mongo_output)
                
                pg_results = {}
                mongo_results = {}
                for result in results:
                    pg_results[result['query_name']] = {
                        'description': result['description'],
                        'explain_result': result['postgresql']
                    }
                    mongo_results[result['query_name']] = {
                        'description': result['description'],
                        'explain_result': result['mongodb']
                    }
                
                with open(pg_path, 'w') as f:
                    json.dump(pg_results, f, indent=2)
                
                with open(mongo_path, 'w') as f:
                    json.dump(mongo_results, f, indent=2, cls=MongoEncoder)
                
                print(f"\nResults saved to {pg_path} and {mongo_path}")
            else:
                save_results_to_json(results, args.pg_output, args.mongo_output)
        else:
            print("No benchmark results to report")
            
    finally:
        close_connections(pg_conn, mongo_client)

if __name__ == "__main__":
    main() 