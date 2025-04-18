CREATE TABLE businesses (
    business_id VARCHAR(22) PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT,
    stars FLOAT,
    review_count INTEGER,
    is_open INTEGER,
    attributes JSONB,
    categories TEXT,
    hours JSONB
);

CREATE TABLE users (
    user_id VARCHAR(22) PRIMARY KEY,
    name VARCHAR(255),
    review_count INTEGER,
    yelping_since TIMESTAMP,
    friends TEXT[],
    useful INTEGER,
    funny INTEGER,
    cool INTEGER,
    fans INTEGER,
    elite INTEGER[],
    average_stars FLOAT,
    compliment_hot INTEGER,
    compliment_more INTEGER,
    compliment_profile INTEGER,
    compliment_cute INTEGER,
    compliment_list INTEGER,
    compliment_note INTEGER,
    compliment_plain INTEGER,
    compliment_cool INTEGER,
    compliment_funny INTEGER,
    compliment_writer INTEGER,
    compliment_photos INTEGER
);

CREATE TABLE reviews (
    review_id VARCHAR(22) PRIMARY KEY,
    user_id VARCHAR(22) REFERENCES users(user_id),
    business_id VARCHAR(22) REFERENCES businesses(business_id),
    stars INTEGER,
    date TIMESTAMP,
    text TEXT,
    useful INTEGER,
    funny INTEGER,
    cool INTEGER
);

CREATE TABLE tips (
    tip_id SERIAL PRIMARY KEY,
    user_id VARCHAR(22) REFERENCES users(user_id),
    business_id VARCHAR(22) REFERENCES businesses(business_id),
    text TEXT,
    date TIMESTAMP,
    compliment_count INTEGER
);

CREATE TABLE checkins (
    checkin_id SERIAL PRIMARY KEY,
    business_id VARCHAR(22) REFERENCES businesses(business_id),
    date TEXT
);
