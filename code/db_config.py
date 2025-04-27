import os

PG_PARAMS = {
    'dbname': os.environ.get('PG_DB', 'yelp_db'), 
    'user': os.environ.get('PG_USER', 'postgres'),  
    'password': os.environ.get('PG_PASSWORD', 'postgres'),
    'host': os.environ.get('PG_HOST', 'postgres'),
    'port': os.environ.get('PG_PORT', '5432')
}

MONGO_PARAMS = {
    'host': os.environ.get('MONGO_HOST', 'mongodb'),
    'port': int(os.environ.get('MONGO_PORT', '27017')),
    'username': os.environ.get('MONGO_USER', 'mongodb'),
    'password': os.environ.get('MONGO_PASSWORD', 'mongodb')
}

def get_mongo_uri():
    return f"mongodb://{MONGO_PARAMS['username']}:{MONGO_PARAMS['password']}@{MONGO_PARAMS['host']}:{MONGO_PARAMS['port']}/"

def get_pg_connection_string():
    return f"postgresql://{PG_PARAMS['user']}:{PG_PARAMS['password']}@{PG_PARAMS['host']}:{PG_PARAMS['port']}/{PG_PARAMS['dbname']}"

DEFAULT_DB_NAME = 'yelp_db' 