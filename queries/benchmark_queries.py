
QUERIES = {
    'business_by_city': {
        'description': 'Find businesses in a specific city',
        'pg': 'SELECT business_id, name, stars, review_count FROM businesses WHERE city = %s LIMIT 100',
        'pg_params': ['Berkeley'],
        'mongo': lambda db: list(db.businesses.find(
            {'city': 'Berkeley'}, 
            {'_id': 1, 'name': 1, 'stars': 1, 'review_count': 1}
        ).limit(100))
    }
}

def get_query(query_name):
    return QUERIES.get(query_name)

def get_all_queries():
    return QUERIES

def list_queries():
    print("\nAvailable benchmark queries:")
    for name, info in QUERIES.items():
        print(f"  - {name}: {info['description']}") 