"""
Benchmark queries for PostgreSQL and MongoDB
Each query should have:
1. description - Text description of what the query does
2. pg - SQL query string for PostgreSQL 
3. pg_params - Parameters for the PostgreSQL query (if needed)
4. mongo - A function that takes a MongoDB database connection and returns query results
5. mongo_explain - A function that takes a MongoDB database connection and returns explain output
"""

QUERIES = {    
    'dancing_restaurants_philly': {
        'description': 'Find restaurants with dancing, reservations and alcohol in Philadelphia',
        'pg': """
            SELECT business_id, name, city, state
            FROM businesses
            WHERE 
            attributes->>'Alcohol' IS NOT NULL
            AND attributes->>'Alcohol' NOT IN ('u''none''', 'None')
            AND attributes->>'GoodForDancing' = 'True'
            AND attributes->>'RestaurantsReservations' = 'True'
            AND attributes->>'RestaurantsGoodForGroups' = 'True'
            AND hours->>'Friday' IS NOT NULL
            AND CAST(SPLIT_PART(SPLIT_PART(hours->>'Friday', '-', 1), ':', 1) AS INTEGER) <= 20
            AND city = 'Philadelphia'
            AND state = 'PA'
            ORDER BY stars DESC, review_count DESC
            LIMIT 10
        """,
        'mongo': lambda db: list(db.businesses.aggregate([
            {'$match': {'attributes.Alcohol': {'$exists': True, '$nin': ["u'none'", 'None']},
            "attributes.RestaurantsReservations": "True",
            "attributes.GoodForDancing": "True",
            "attributes.RestaurantsGoodForGroups": "True",
            'hours.Friday': {'$exists': True},
            'review_count': {'$gte': 5},
            'city': 'Philadelphia',
            'state': 'PA',
            '$or': [{'categories': {'$regex': 'Bar', '$options': 'i'}}, 
                   {'categories': {'$regex': 'Lounge', '$options': 'i'}}]}}, 
            {'$addFields': {'open_hour': {'$toInt': {'$arrayElemAt': 
            [{'$split': [{'$arrayElemAt': 
            [{'$split': ['$hours.Friday', '-']}, 0]},':']}, 0]}}}},
            {'$match': {'open_hour': {'$lte': 20}}},
            {'$sort': {'stars': -1, 'review_count': -1}},
            {'$project': {'business_id': 1, 
            'name': 1,
            'stars': 1}},
            {'$limit': 10}
        ])),
        'mongo_explain': lambda db: db.command(
            'explain',
            {
                'aggregate': 'businesses',
                'pipeline': [
                    {'$match': {
                        'attributes.Alcohol': {'$exists': True, '$nin': ["u'none'", 'None']},
                        "attributes.RestaurantsReservations": "True",
                        "attributes.GoodForDancing": "True",
                        "attributes.RestaurantsGoodForGroups": "True",
                        'hours.Friday': {'$exists': True},
                        'review_count': {'$gte': 5},
                        'city': 'Philadelphia',
                        'state': 'PA',
                        '$or': [
                            {'categories': {'$regex': 'Bar', '$options': 'i'}}, 
                            {'categories': {'$regex': 'Lounge', '$options': 'i'}}
                        ]
                    }}, 
                    {'$addFields': {
                        'open_hour': {'$toInt': {'$arrayElemAt': 
                        [{'$split': [{'$arrayElemAt': 
                        [{'$split': ['$hours.Friday', '-']}, 0]},':']}, 0]}}
                    }},
                    {'$match': {'open_hour': {'$lte': 20}}},
                    {'$sort': {'stars': -1, 'review_count': -1}},
                    {'$project': {
                        'business_id': 1, 
                        'name': 1,
                        'stars': 1
                    }},
                    {'$limit': 10}
                ],
                'cursor': {}
            },
            verbosity='executionStats'  
        )
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