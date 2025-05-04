
from db_config import PG_PARAMS, get_mongo_uri, DEFAULT_DB_NAME
import psycopg2
from pymongo import MongoClient

def remove_postgres_indexes():
    conn = psycopg2.connect(**PG_PARAMS)
    cur = conn.cursor()
    print("Dropping PostgreSQL indexes...")
    cur.execute("""
        DROP INDEX IF EXISTS idx_businesses_city;
        DROP INDEX IF EXISTS idx_businesses_stars;
        DROP INDEX IF EXISTS idx_businesses_categories;
        DROP INDEX IF EXISTS idx_reviews_user_id;
        DROP INDEX IF EXISTS idx_reviews_business_id;
        DROP INDEX IF EXISTS idx_reviews_stars;
        DROP INDEX IF EXISTS idx_reviews_date;
        DROP INDEX IF EXISTS idx_tips_user_id;
        DROP INDEX IF EXISTS idx_tips_business_id;
        DROP INDEX IF EXISTS idx_checkins_business_id;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("🗑️ PostgreSQL indexes removed.")

def remove_mongo_indexes():
    client = MongoClient(get_mongo_uri())
    db = client[DEFAULT_DB_NAME]
    print("Dropping MongoDB indexes...")

    db.businesses.drop_indexes()
    db.users.drop_indexes()
    db.reviews.drop_indexes()
    db.tips.drop_indexes()
    db.checkins.drop_indexes()

    client.close()
    print("🗑️ MongoDB indexes removed.")

if __name__ == "__main__":
    remove_postgres_indexes()
    remove_mongo_indexes()