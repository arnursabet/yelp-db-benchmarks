import json
import os
import sys
import argparse
import pymongo
import hashlib

from db_config import MONGO_PARAMS, get_mongo_uri, DEFAULT_DB_NAME

parser = argparse.ArgumentParser(description='Load Yelp dataset into MongoDB')
parser.add_argument('--collections', nargs='+', default=['all'], 
                    choices=['all', 'businesses', 'users', 'reviews', 'tips', 'checkins'],
                    help='Specify which collections to load (default: all)')
parser.add_argument('--skip-validation', action='store_true',
                    help='Skip validation of user and business IDs (faster but may include invalid references)')
args = parser.parse_args()

data_dir = './data/yelp_dataset/'

print(f"Loading collections: {args.collections if 'all' not in args.collections else 'all'}")
print(f"Skip validation: {args.skip_validation}")

print("Connecting to MongoDB...")
client = pymongo.MongoClient(get_mongo_uri())

mongo_db = client[DEFAULT_DB_NAME]

# Function to load documents in batches
def load_mongo_batch(collection, documents, batch_size=1000):
    total_loaded = 0
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        try:
            result = collection.insert_many(batch, ordered=False)
            total_loaded += len(result.inserted_ids)
            print(f"  Inserted {total_loaded} documents...")
        except pymongo.errors.BulkWriteError as e:
            # Some documents may have been inserted before the error
            result = e.details
            total_loaded += result.get('nInserted', 0)
            print(f"  Warning: {len(result.get('writeErrors', []))} errors. Inserted {result.get('nInserted', 0)} documents.")
    return total_loaded

def load_businesses():
    # Only drop the collection if explicitly loading it
    if 'all' in args.collections or 'businesses' in args.collections:
        print("Dropping businesses collection...")
        mongo_db.businesses.drop()
        
        print("Loading businesses into MongoDB...")
        businesses_collection = mongo_db.businesses
        businesses = []
        total_businesses = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_business.json'), 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                data['_id'] = data.pop('business_id')
                businesses.append(data)
                
                if len(businesses) >= 10000:
                    loaded = load_mongo_batch(businesses_collection, businesses)
                    total_businesses += loaded
                    businesses = []

        # Load any remaining businesses
        if businesses:
            loaded = load_mongo_batch(businesses_collection, businesses)
            total_businesses += loaded

        print(f"Total businesses loaded: {total_businesses}")
        
        return businesses_collection
    else:
        print("Skipping businesses collection...")
        return mongo_db.businesses

def load_users():
    # Only drop the collection if explicitly loading it
    if 'all' in args.collections or 'users' in args.collections:
        print("Dropping users collection...")
        mongo_db.users.drop()
        
        print("Loading users into MongoDB...")
        users_collection = mongo_db.users
        users = []
        total_users = 0

        with open(os.path.join(data_dir, 'yelp_academic_dataset_user.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    data['_id'] = data.pop('user_id')
                    users.append(data)
                    
                    if len(users) >= 10000:
                        loaded = load_mongo_batch(users_collection, users)
                        total_users += loaded
                        users = []
                        print(f"  Processed {total_users} users so far...")
                        
                except Exception as e:
                    print(f"Error processing user: {e}")
                    continue

        # Load any remaining users
        if users:
            loaded = load_mongo_batch(users_collection, users)
            total_users += loaded

        print(f"Total users loaded: {total_users}")
        
        return users_collection
    else:
        print("Skipping users collection...")
        return mongo_db.users

# Function to collect valid business and user IDs
def collect_valid_ids(businesses_collection, users_collection):
    if args.skip_validation:
        print("Skipping ID validation as requested...")
        return set(), set()
    
    print("Collecting valid business IDs...")
    valid_business_ids = set()
    for business in businesses_collection.find({}, {"_id": 1}):
        valid_business_ids.add(business["_id"])
    print(f"Collected {len(valid_business_ids)} valid business IDs")

    # Collect valid user IDs in batches to avoid memory issues
    print("Collecting valid user IDs in batches...")
    valid_user_ids = set()
    batch_size = 100000
    user_cursor = users_collection.find({}, {"_id": 1})
    processed = 0

    for user in user_cursor:
        valid_user_ids.add(user["_id"])
        processed += 1
        if processed % batch_size == 0:
            print(f"Processed {processed} user IDs...")

    print(f"Collected {len(valid_user_ids)} valid user IDs")
    return valid_business_ids, valid_user_ids

def load_reviews(valid_business_ids, valid_user_ids):
    if 'all' in args.collections or 'reviews' in args.collections:
        print("Dropping reviews collection...")
        mongo_db.reviews.drop()
        
        print("Loading reviews into MongoDB...")
        reviews_collection = mongo_db.reviews
        reviews = []
        total_loaded = 0
        total_skipped = 0
        batch_size = 10000

        with open(os.path.join(data_dir, 'yelp_academic_dataset_review.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if args.skip_validation or (data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids):
                        data['_id'] = data.pop('review_id')
                        reviews.append(data)
                        
                        if len(reviews) >= batch_size:
                            loaded = load_mongo_batch(reviews_collection, reviews, batch_size=1000)
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
            loaded = load_mongo_batch(reviews_collection, reviews, batch_size=1000)
            total_loaded += loaded

        print(f"Total reviews loaded: {total_loaded}, skipped: {total_skipped}")
        
        return reviews_collection
    else:
        print("Skipping reviews collection...")
        return mongo_db.reviews

def load_tips(valid_business_ids, valid_user_ids):
    if 'all' in args.collections or 'tips' in args.collections:
        print("Dropping tips collection...")
        mongo_db.tips.drop()
        
        print("Loading tips into MongoDB...")
        tips_collection = mongo_db.tips
        tips = []
        total_loaded = 0
        total_skipped = 0
        batch_size = 10000

        # Track seen tip hashes to avoid duplicates
        seen_tips = set()

        with open(os.path.join(data_dir, 'yelp_academic_dataset_tip.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if args.skip_validation or (data['user_id'] in valid_user_ids and data['business_id'] in valid_business_ids):
                        text_hash = hashlib.md5(data['text'].encode()).hexdigest()[:8]
                        unique_id = f"{data['user_id']}_{data['business_id']}_{data['date']}_{text_hash}"
                        
                        if unique_id in seen_tips:
                            total_skipped += 1
                            continue
                            
                        seen_tips.add(unique_id)
                        data['_id'] = unique_id
                        tips.append(data)
                        
                        if len(tips) >= batch_size:
                            # Clear seen_tips set to save memory after each batch
                            seen_tips.clear()
                            loaded = load_mongo_batch(tips_collection, tips, batch_size=1000)
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
            loaded = load_mongo_batch(tips_collection, tips, batch_size=1000)
            total_loaded += loaded

        print(f"Total tips loaded: {total_loaded}, skipped: {total_skipped}")
        
        return tips_collection
    else:
        print("Skipping tips collection...")
        return mongo_db.tips

def load_checkins(valid_business_ids):
    if 'all' in args.collections or 'checkins' in args.collections:
        print("Dropping checkins collection...")
        mongo_db.checkins.drop()
        
        print("Loading checkins into MongoDB...")
        checkins_collection = mongo_db.checkins
        checkins = []
        total_loaded = 0
        total_skipped = 0
        batch_size = 5000

        with open(os.path.join(data_dir, 'yelp_academic_dataset_checkin.json'), 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if args.skip_validation or data['business_id'] in valid_business_ids:
                        data['_id'] = data['business_id']
                        checkins.append(data)
                        
                        if len(checkins) >= batch_size:
                            loaded = load_mongo_batch(checkins_collection, checkins, batch_size=1000)
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

        # Load any remaining checkins
        if checkins:
            loaded = load_mongo_batch(checkins_collection, checkins, batch_size=1000)
            total_loaded += loaded

        print(f"Total checkins loaded: {total_loaded}, skipped: {total_skipped}")

        return checkins_collection
    else:
        print("Skipping checkins collection...")
        return mongo_db.checkins

def main():
    try:
        businesses_collection = load_businesses()
        users_collection = load_users()
        
        need_validation = not args.skip_validation and ('all' in args.collections or 
                                                      'reviews' in args.collections or 
                                                      'tips' in args.collections or 
                                                      'checkins' in args.collections)
        
        valid_business_ids, valid_user_ids = collect_valid_ids(businesses_collection, users_collection) if need_validation else (set(), set())
        
        reviews_collection = load_reviews(valid_business_ids, valid_user_ids)
        tips_collection = load_tips(valid_business_ids, valid_user_ids)
        checkins_collection = load_checkins(valid_business_ids)
        
        print("MongoDB data loading complete!")
    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())