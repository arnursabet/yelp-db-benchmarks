import json
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
from pymongo import MongoClient
from psycopg2.extensions import adapt, register_adapter, AsIs

params = {
    'dbname': 'postgres', 
    'user': 'postgres',  
    'password': 'postgres',
    'host': 'postgres',
    'port': '5432'
}

print("Connecting to PostgreSQL...")
conn = psycopg2.connect(**params)
conn.autocommit = True
cursor = conn.cursor()

print("Creating fresh database...")
cursor.execute("DROP DATABASE IF EXISTS yelp_db")
cursor.execute("CREATE DATABASE yelp_db")
conn.close()

params['dbname'] = 'yelp_db'
conn = psycopg2.connect(**params)
cursor = conn.cursor()

print("Creating tables...")
with open('./queries/schema.sql', 'r') as f:
    create_tables_sql = f.read()
    cursor.execute(create_tables_sql)
conn.commit()

print("Enabling pg_trgm extension...")
cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
conn.commit()

data_dir = './data/yelp_dataset/'

# Define a function for batch processing
def batch_insert(data_list, insert_query, batch_size=5000):
    total_processed = 0
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i+batch_size]
        execute_values(cursor, insert_query, batch)
        conn.commit()
        total_processed += len(batch)
        print(f"  Inserted {total_processed} records...")
    return total_processed

# Define a function to parse dates safely
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

print("Loading businesses...")
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
total_businesses = batch_insert(businesses, business_query)
print(f"Loaded {total_businesses} businesses")

print("Loading users...")
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
                batch_loaded = batch_insert(users, user_query)
                total_users += batch_loaded
                users = []
                print(f"  Processed {total_users} users so far...")
                
        except Exception as e:
            print(f"Error processing user: {e}")
            continue

# Insert any remaining users
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
    batch_loaded = batch_insert(users, user_query)
    total_users += batch_loaded
    print(f"  Processed final batch of users")

print(f"Total users loaded: {total_users}")

# Create temporary indices to speed up review validation
print("Creating temporary indices for validation...")
cursor.execute("CREATE INDEX temp_idx_business_id ON businesses(business_id)")
cursor.execute("CREATE INDEX temp_idx_user_id ON users(user_id)")
conn.commit()

# Prepare for reviews loading - collect valid IDs
print("Collecting valid business and user IDs...")
cursor.execute("SELECT business_id FROM businesses")
valid_business_ids = {row[0] for row in cursor.fetchall()}

cursor.execute("SELECT user_id FROM users")
valid_user_ids = {row[0] for row in cursor.fetchall()}

# Load reviews with validation
print("Loading reviews...")
reviews = []
batch_size = 10000
total_loaded = 0
total_skipped = 0

with open(os.path.join(data_dir, 'yelp_academic_dataset_review.json'), 'r', encoding='utf-8') as f:
    for line in f:
        try:
            data = json.loads(line)
            # Only add reviews where both user_id and business_id exist in our database
            if data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids:
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
                    execute_values(cursor, query, reviews)
                    conn.commit()
                    
                    total_loaded += len(reviews)
                    reviews = []
                    print(f"  Loaded {total_loaded} reviews...")
            else:
                total_skipped += 1
        except Exception as e:
            print(f"Error processing review: {e}")
            total_skipped += 1
            continue

# Insert any remaining reviews
if reviews:
    query = """
    INSERT INTO reviews (
        review_id, user_id, business_id, stars,
        date, text, useful, funny, cool
    ) VALUES %s
    """
    execute_values(cursor, query, reviews)
    conn.commit()
    
    total_loaded += len(reviews)

print(f"Total reviews loaded: {total_loaded}, skipped: {total_skipped}")

# Load tips with validation
print("Loading tips...")
tips = []
batch_size = 10000
total_loaded = 0
total_skipped = 0

with open(os.path.join(data_dir, 'yelp_academic_dataset_tip.json'), 'r', encoding='utf-8') as f:
    for line in f:
        try:
            data = json.loads(line)
            # Only add tips where both user_id and business_id exist in our database
            if data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids:
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
                    execute_values(cursor, query, tips)
                    conn.commit()
                    
                    total_loaded += len(tips)
                    tips = []
                    print(f"  Loaded {total_loaded} tips...")
            else:
                total_skipped += 1
        except Exception as e:
            print(f"Error processing tip: {e}")
            total_skipped += 1
            continue

# Insert any remaining tips
if tips:
    query = """
    INSERT INTO tips (
        user_id, business_id, text, date, compliment_count
    ) VALUES %s
    """
    execute_values(cursor, query, tips)
    conn.commit()
    
    total_loaded += len(tips)

print(f"Total tips loaded: {total_loaded}, skipped: {total_skipped}")

# Load checkins with validation
print("Loading checkins...")
checkins = []
batch_size = 5000
total_loaded = 0
total_skipped = 0

with open(os.path.join(data_dir, 'yelp_academic_dataset_checkin.json'), 'r', encoding='utf-8') as f:
    for line in f:
        try:
            data = json.loads(line)
            # Only add checkins where business_id exists in our database
            if data['business_id'] in valid_business_ids:
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
                    execute_values(cursor, query, checkins)
                    conn.commit()
                    
                    total_loaded += len(checkins)
                    checkins = []
                    print(f"  Loaded {total_loaded} checkins...")
            else:
                total_skipped += 1
        except Exception as e:
            print(f"Error processing checkin: {e}")
            total_skipped += 1
            continue

# Insert any remaining checkins
if checkins:
    query = """
    INSERT INTO checkins (
        business_id, date
    ) VALUES %s
    """
    execute_values(cursor, query, checkins)
    conn.commit()
    
    total_loaded += len(checkins)

print(f"Total checkins loaded: {total_loaded}, skipped: {total_skipped}")

# Drop temporary indices
print("Dropping temporary indices...")
cursor.execute("DROP INDEX IF EXISTS temp_idx_business_id")
cursor.execute("DROP INDEX IF EXISTS temp_idx_user_id")
conn.commit()

# Create final indices for queries
print("Creating final indices...")
cursor.execute("""
CREATE INDEX idx_businesses_city ON businesses(city);
CREATE INDEX idx_businesses_stars ON businesses(stars);
CREATE INDEX idx_businesses_categories ON businesses USING gin(to_tsvector('english', coalesce(categories, '')));
CREATE INDEX idx_reviews_user_id ON reviews(user_id);
CREATE INDEX idx_reviews_business_id ON reviews(business_id);
CREATE INDEX idx_reviews_stars ON reviews(stars);
CREATE INDEX idx_reviews_date ON reviews(date);
CREATE INDEX idx_tips_user_id ON tips(user_id);
CREATE INDEX idx_tips_business_id ON tips(business_id);
CREATE INDEX idx_checkins_business_id ON checkins(business_id);
""")
conn.commit()
print("Indices created successfully")

conn.close()
print("PostgreSQL data loading complete!")

print("Now setting up MongoDB...")
# Todo: Implement MongoDB loading
client = MongoClient('mongodb://mongodb:27017/')
print("All data loading complete") 