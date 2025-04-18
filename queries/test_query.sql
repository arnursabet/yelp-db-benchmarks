-- Query 1: Top-rated restaurants with at least 100 reviews
SELECT 
    b.business_id,
    b.name,
    b.city,
    b.state,
    b.stars AS average_rating,
    b.review_count
FROM 
    businesses b
WHERE 
    b.categories LIKE '%Restaurant%' 
    AND b.review_count >= 100
ORDER BY 
    b.stars DESC, b.review_count DESC
LIMIT 10;

-- Query 2: Count records in each table
SELECT 'businesses' AS table_name, COUNT(*) AS record_count FROM businesses
UNION ALL
SELECT 'users' AS table_name, COUNT(*) AS record_count FROM users
UNION ALL
SELECT 'reviews' AS table_name, COUNT(*) AS record_count FROM reviews
UNION ALL
SELECT 'tips' AS table_name, COUNT(*) AS record_count FROM tips
UNION ALL
SELECT 'checkins' AS table_name, COUNT(*) AS record_count FROM checkins
UNION ALL
SELECT 'photos' AS table_name, COUNT(*) AS record_count FROM photos
ORDER BY table_name;

-- Query 3: Find user with most reviews
SELECT 
    u.user_id,
    u.name,
    u.review_count,
    u.fans,
    u.average_stars
FROM 
    users u
ORDER BY 
    u.review_count DESC
LIMIT 10;

-- Query 4: Average stars by city (for cities with at least 100 businesses)
SELECT 
    b.city,
    b.state,
    COUNT(*) as business_count,
    AVG(b.stars) as average_stars
FROM 
    businesses b
GROUP BY 
    b.city, b.state
HAVING 
    COUNT(*) >= 100
ORDER BY 
    AVG(b.stars) DESC
LIMIT 10;

-- Query 5: Distribution of business categories
SELECT 
    category,
    COUNT(*) as category_count
FROM 
    (
        SELECT 
            unnest(string_to_array(categories, ',')) as category
        FROM 
            businesses
    ) as category_data
GROUP BY 
    category
ORDER BY 
    COUNT(*) DESC
LIMIT 20; 