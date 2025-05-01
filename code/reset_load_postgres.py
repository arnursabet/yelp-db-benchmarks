import json
import psycopg2
from psycopg2.extras import execute_values
import os
import sys
import argparse
from datetime import datetime
from pymongo import MongoClient

from db_config import PG_PARAMS, DEFAULT_DB_NAME

parser = argparse.ArgumentParser(description='Load Yelp dataset into PostgreSQL')
parser.add_argument('--tables', nargs='+', default=['all'], 
                    choices=['all', 'businesses', 'users', 'reviews', 'tips', 'checkins'],
                    help='Specify which tables to load (default: all)')
parser.add_argument('--skip-validation', action='store_true',
                    help='Skip validation of user and business IDs (faster but may include invalid references)')
parser.add_argument('--drop-db', action='store_true',
                    help='Drop and recreate the entire database (default: False)')
args = parser.parse_args()

initial_params = PG_PARAMS.copy()

if 'dbname' in initial_params:
    initial_params['dbname'] = 'postgres'

data_dir = './data/yelp_dataset/'

print(f"Loading tables: {args.tables if 'all' not in args.tables else 'all'}")
print(f"Skip validation: {args.skip_validation}")
print(f"Drop database: {args.drop_db}")

def batch_insert(cursor, conn, data_list, insert_query, batch_size=5000):
    total_processed = 0
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i+batch_size]
        try:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_processed += len(batch)
            print(f"  Inserted {total_processed} records...")
        except Exception as e:
            conn.rollback()
            print(f"Error in batch insert: {e}")
    return total_processed

def safe_parse_date(date_str, default='2010-01-01'):
    if not date_str:
        return datetime.strptime(default, '%Y-%m-%d')
    
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return datetime.strptime(default, '%Y-%m-%d')

def setup_database():
    """Set up the PostgreSQL database"""
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**initial_params)
    conn.autocommit = True
    cursor = conn.cursor()

    if args.drop_db:
        print("Creating fresh database...")
        cursor.execute(f"DROP DATABASE IF EXISTS {DEFAULT_DB_NAME}")
        cursor.execute(f"CREATE DATABASE {DEFAULT_DB_NAME}")
        conn.close()

        conn = psycopg2.connect(**PG_PARAMS)
        cursor = conn.cursor()

        print("Creating tables...")
        with open('./queries/schema.sql', 'r') as f:
            create_tables_sql = f.read()
            cursor.execute(create_tables_sql)
        conn.commit()

        print("Enabling pg_trgm extension...")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        conn.commit()
    else:
        # Just connect to yelp_db
        try:
            conn.close()
            conn = psycopg2.connect(**PG_PARAMS)
            cursor = conn.cursor()
        except psycopg2.OperationalError:
            # Database doesn't exist, create it
            conn = psycopg2.connect(**initial_params)
            conn.autocommit = True
            cursor = conn.cursor()
            print(f"Creating database {DEFAULT_DB_NAME} as it doesn't exist...")
            cursor.execute(f"CREATE DATABASE {DEFAULT_DB_NAME}")
            conn.close()
            
            conn = psycopg2.connect(**PG_PARAMS)
            cursor = conn.cursor()
            
            print("Creating tables...")
            with open('./queries/schema.sql', 'r') as f:
                create_tables_sql = f.read()
                cursor.execute(create_tables_sql)
            conn.commit()
            
            print("Enabling pg_trgm extension...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            conn.commit()

    return conn, cursor

def load_businesses(conn, cursor):
    """Load businesses into PostgreSQL"""
    if 'all' in args.tables or 'businesses' in args.tables:
        print("Loading businesses...")
        
        # Check if table already has data
        cursor.execute("SELECT COUNT(*) FROM businesses")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Businesses table already has {count} records. Truncating...")
            cursor.execute("TRUNCATE TABLE businesses CASCADE")
            conn.commit()
        
        businesses = []

        with open(os.path.join(data_dir, 'yelp_academic_dataset_business.json'), 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                businesses.append((
                    data['business_id'],
                    data.get('name', ''),
                    data.get('address', ''),
                    data.get('city', ''),
                    data.get('state', ''),
                    data.get('postal_code', ''),
                    data.get('latitude', 0),
                    data.get('longitude', 0),
                    data.get('stars', 0),
                    data.get('review_count', 0),
                    data.get('is_open', 0),
                    json.dumps(data.get('attributes', {})),
                    data.get('categories', ''),
                    json.dumps(data.get('hours', {}))
                ))

        business_query = """
        INSERT INTO businesses (
            business_id, name, address, city, state, postal_code, 
            latitude, longitude, stars, review_count, is_open, 
            attributes, categories, hours
        ) VALUES %s
        """
        total_businesses = batch_insert(cursor, conn, businesses, business_query)
        print(f"Loaded {total_businesses} businesses")
    else:
        print("Skipping businesses table...")

def load_users(conn, cursor):
    """Load users into PostgreSQL"""
    if 'all' in args.tables or 'users' in args.tables:
        print("Loading users...")
        
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Users table already has {count} records. Truncating...")
            cursor.execute("TRUNCATE TABLE users CASCADE")
            conn.commit()
        
        users = []
        total_users = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_user.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    
                    # Parse friends list
                    friends_list = data.get('friends', '')
                    if isinstance(friends_list, str) and friends_list:
                        friends_list = friends_list.split(', ')
                    elif not isinstance(friends_list, list):
                        friends_list = []
                        
                    # Handle elite years array
                    elite_years = data.get('elite', '')
                    if isinstance(elite_years, str) and elite_years:
                        elite_years = [int(year) for year in elite_years.split(',') if year.strip()]
                    elif not isinstance(elite_years, list):
                        elite_years = []
                    
                    users.append((
                        data['user_id'],
                        data.get('name', ''),
                        data.get('review_count', 0),
                        safe_parse_date(data.get('yelping_since')),
                        friends_list,
                        data.get('useful', 0),
                        data.get('funny', 0),
                        data.get('cool', 0),
                        data.get('fans', 0),
                        elite_years,
                        data.get('average_stars', 0),
                        data.get('compliment_hot', 0),
                        data.get('compliment_more', 0),
                        data.get('compliment_profile', 0),
                        data.get('compliment_cute', 0),
                        data.get('compliment_list', 0),
                        data.get('compliment_note', 0),
                        data.get('compliment_plain', 0),
                        data.get('compliment_cool', 0),
                        data.get('compliment_funny', 0),
                        data.get('compliment_writer', 0),
                        data.get('compliment_photos', 0)
                    ))
                    
                    if len(users) >= 10000:
                        user_query = """
                        INSERT INTO users (
                            user_id, name, review_count, yelping_since, friends,
                            useful, funny, cool, fans, elite, average_stars,
                            compliment_hot, compliment_more, compliment_profile, compliment_cute,
                            compliment_list, compliment_note, compliment_plain, compliment_cool,
                            compliment_funny, compliment_writer, compliment_photos
                        ) VALUES %s
                        """
                        batch_loaded = batch_insert(cursor, conn, users, user_query)
                        total_users += batch_loaded
                        users = []
                        print(f"  Processed {total_users} users so far...")
                        
                except Exception as e:
                    print(f"Error processing user: {e}")
                    continue

        if users:
            user_query = """
            INSERT INTO users (
                user_id, name, review_count, yelping_since, friends,
                useful, funny, cool, fans, elite, average_stars,
                compliment_hot, compliment_more, compliment_profile, compliment_cute,
                compliment_list, compliment_note, compliment_plain, compliment_cool,
                compliment_funny, compliment_writer, compliment_photos
            ) VALUES %s
            """
            batch_loaded = batch_insert(cursor, conn, users, user_query)
            total_users += batch_loaded
            print(f"  Processed final batch of users")

        print(f"Total users loaded: {total_users}")
    else:
        print("Skipping users table...")

def collect_valid_ids(conn, cursor):
    """Collect valid business and user IDs for validation"""
    if args.skip_validation:
        print("Skipping ID validation as requested...")
        return set(), set()

    print("Collecting valid business and user IDs...")
    cursor.execute("SELECT business_id FROM businesses")
    valid_business_ids = {row[0] for row in cursor.fetchall()}
    print(f"Collected {len(valid_business_ids)} valid business IDs")

    cursor.execute("SELECT user_id FROM users")
    valid_user_ids = {row[0] for row in cursor.fetchall()}
    print(f"Collected {len(valid_user_ids)} valid user IDs")
    
    return valid_business_ids, valid_user_ids

def load_reviews(conn, cursor, valid_business_ids, valid_user_ids):
    """Load reviews into PostgreSQL"""
    if 'all' in args.tables or 'reviews' in args.tables:
        print("Loading reviews...")
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Reviews table already has {count} records. Truncating...")
            cursor.execute("TRUNCATE TABLE reviews CASCADE")
            conn.commit()
        
        reviews = []
        batch_size = 10000
        total_loaded = 0
        total_skipped = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_review.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if args.skip_validation or (data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids):
                        reviews.append((
                            data['review_id'],
                            data['user_id'],
                            data['business_id'],
                            data.get('stars', 0),
                            safe_parse_date(data.get('date')),
                            data.get('text', ''),
                            data.get('useful', 0),
                            data.get('funny', 0),
                            data.get('cool', 0)
                        ))
                        
                        if len(reviews) >= batch_size:
                            query = """
                            INSERT INTO reviews (
                                review_id, user_id, business_id, stars,
                                date, text, useful, funny, cool
                            ) VALUES %s
                            """
                            loaded = batch_insert(cursor, conn, reviews, query)
                            total_loaded += loaded
                            reviews = []
                            print(f"  Loaded {total_loaded} reviews...")
                    else:
                        total_skipped += 1
                        if total_skipped % 10000 == 0:
                            print(f"  Skipped {total_skipped} reviews so far...")
                except Exception as e:
                    print(f"Error processing review: {e}")
                    total_skipped += 1
                    continue

        if reviews:
            query = """
            INSERT INTO reviews (
                review_id, user_id, business_id, stars,
                date, text, useful, funny, cool
            ) VALUES %s
            """
            loaded = batch_insert(cursor, conn, reviews, query)
            total_loaded += loaded

        print(f"Total reviews loaded: {total_loaded}, skipped: {total_skipped}")
    else:
        print("Skipping reviews table...")

def load_tips(conn, cursor, valid_business_ids, valid_user_ids):
    """Load tips into PostgreSQL"""
    if 'all' in args.tables or 'tips' in args.tables:
        print("Loading tips...")
        
        cursor.execute("SELECT COUNT(*) FROM tips")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Tips table already has {count} records. Truncating...")
            cursor.execute("TRUNCATE TABLE tips CASCADE")
            conn.commit()
        
        tips = []
        batch_size = 10000
        total_loaded = 0
        total_skipped = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_tip.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if args.skip_validation or (data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids):
                        tips.append((
                            data['user_id'],
                            data['business_id'],
                            data.get('text', ''),
                            safe_parse_date(data.get('date')),
                            data.get('compliment_count', 0)
                        ))
                        
                        if len(tips) >= batch_size:
                            query = """
                            INSERT INTO tips (
                                user_id, business_id, text, date, compliment_count
                            ) VALUES %s
                            """
                            loaded = batch_insert(cursor, conn, tips, query)
                            total_loaded += loaded
                            tips = []
                            print(f"  Loaded {total_loaded} tips...")
                    else:
                        total_skipped += 1
                        if total_skipped % 10000 == 0:
                            print(f"  Skipped {total_skipped} tips so far...")
                except Exception as e:
                    print(f"Error processing tip: {e}")
                    total_skipped += 1
                    continue

        if tips:
            query = """
            INSERT INTO tips (
                user_id, business_id, text, date, compliment_count
            ) VALUES %s
            """
            loaded = batch_insert(cursor, conn, tips, query)
            total_loaded += loaded

        print(f"Total tips loaded: {total_loaded}, skipped: {total_skipped}")
    else:
        print("Skipping tips table...")

def load_checkins(conn, cursor, valid_business_ids):
    """Load checkins into PostgreSQL"""
    if 'all' in args.tables or 'checkins' in args.tables:
        print("Loading checkins...")
        
        cursor.execute("SELECT COUNT(*) FROM checkins")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Checkins table already has {count} records. Truncating...")
            cursor.execute("TRUNCATE TABLE checkins CASCADE")
            conn.commit()
        
        checkins = []
        batch_size = 5000
        total_loaded = 0
        total_skipped = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_checkin.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    # Only add checkins where business_id exists in our database
                    if args.skip_validation or data['business_id'] in valid_business_ids:
                        checkins.append((
                            data['business_id'],
                            data.get('date', '')
                        ))
                        
                        if len(checkins) >= batch_size:
                            query = """
                            INSERT INTO checkins (
                                business_id, date
                            ) VALUES %s
                            """
                            loaded = batch_insert(cursor, conn, checkins, query)
                            total_loaded += loaded
                            checkins = []
                            print(f"  Loaded {total_loaded} checkins...")
                    else:
                        total_skipped += 1
                        if total_skipped % 5000 == 0:
                            print(f"  Skipped {total_skipped} checkins so far...")
                except Exception as e:
                    print(f"Error processing checkin: {e}")
                    total_skipped += 1
                    continue

        if checkins:
            query = """
            INSERT INTO checkins (
                business_id, date
            ) VALUES %s
            """
            loaded = batch_insert(cursor, conn, checkins, query)
            total_loaded += loaded

        print(f"Total checkins loaded: {total_loaded}, skipped: {total_skipped}")
    else:
        print("Skipping checkins table...")


def main():
    try:
        conn, cursor = setup_database()
        
        load_businesses(conn, cursor)
        load_users(conn, cursor)
        
        need_validation = not args.skip_validation and ('all' in args.tables or 
                                                     'reviews' in args.tables or 
                                                     'tips' in args.tables or 
                                                     'checkins' in args.tables)
        
        valid_business_ids, valid_user_ids = collect_valid_ids(conn, cursor) if need_validation else (set(), set())
        
        load_reviews(conn, cursor, valid_business_ids, valid_user_ids)
        load_tips(conn, cursor, valid_business_ids, valid_user_ids)
        load_checkins(conn, cursor, valid_business_ids)
        
        conn.close()
        print("PostgreSQL data loading complete!")
        return 0
    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())