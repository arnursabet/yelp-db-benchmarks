-- Query 1: Find the top 10 restaurants by average rating with at least 10 reviews
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
    AND b.review_count >= 10
ORDER BY 
    b.stars DESC, b.review_count DESC
LIMIT 10;

-- Query 2: Find users who have written the most reviews
SELECT 
    u.user_id,
    u.name,
    u.review_count,
    u.yelping_since,
    u.fans
FROM 
    users u
ORDER BY 
    u.review_count DESC
LIMIT 20;

-- Query 3: Find the distribution of star ratings for a specific city
SELECT 
    b.stars,
    COUNT(*) as count
FROM 
    businesses b
WHERE 
    b.city = 'Las Vegas'
GROUP BY 
    b.stars
ORDER BY 
    b.stars;
