
from db_config import PG_PARAMS, get_mongo_uri, DEFAULT_DB_NAME
import psycopg2
from pymongo import MongoClient

def add_postgres_indexes():
    conn = psycopg2.connect(**PG_PARAMS)
    cur = conn.cursor()
    print("Creating PostgreSQL indexes...")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_businesses_city ON businesses(city);
        CREATE INDEX IF NOT EXISTS idx_businesses_stars ON businesses(stars);
        CREATE INDEX IF NOT EXISTS idx_businesses_categories ON businesses USING gin(to_tsvector('english', coalesce(categories, '')));
        CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_business_id ON reviews(business_id);
        CREATE INDEX IF NOT EXISTS idx_reviews_stars ON reviews(stars);
        CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(date);
        CREATE INDEX IF NOT EXISTS idx_tips_user_id ON tips(user_id);
        CREATE INDEX IF NOT EXISTS idx_tips_business_id ON tips(business_id);
        CREATE INDEX IF NOT EXISTS idx_checkins_business_id ON checkins(business_id);
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ PostgreSQL indexes added.")

def add_mongo_indexes():
    client = MongoClient(get_mongo_uri())
    db = client[DEFAULT_DB_NAME]
    print("Creating MongoDB indexes...")

    db.businesses.create_index([("name", 1)])
    db.businesses.create_index([("city", 1)])
    db.businesses.create_index([("stars", 1)])
    db.businesses.create_index([("categories", "text")])
    db.businesses.create_index([("coordinates", "2dsphere")])

    db.users.create_index([("review_count", 1)])
    db.users.create_index([("yelping_since", 1)])
    db.users.create_index([("average_stars", 1)])

    db.reviews.create_index([("business_id", 1)])
    db.reviews.create_index([("user_id", 1)])
    db.reviews.create_index([("stars", 1)])
    db.reviews.create_index([("date", 1)])

    db.tips.create_index([("business_id", 1)])
    db.tips.create_index([("user_id", 1)])
    db.tips.create_index([("date", 1)])

    db.checkins.create_index([("business_id", 1)])

    client.close()
    print("✅ MongoDB indexes added.")

if __name__ == "__main__":
    add_postgres_indexes()
    add_mongo_indexes()
