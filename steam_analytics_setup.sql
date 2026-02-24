-- creating schemas

CREATE SCHEMA staging;
CREATE SCHEMA core;
CREATE SCHEMA analytics;


-- creating tables, setting up tables for csv import

CREATE TABLE staging.app_details (
    appid INTEGER,
    name TEXT,
    type TEXT,
    is_free BOOLEAN,
    coming_soon BOOLEAN,
    release_date TEXT,
    price INTEGER,
    recommendations INTEGER,
    developers TEXT,
    publishers TEXT
);

CREATE TABLE staging.app_genres (
    appid INTEGER,
    genre_id INTEGER
);

CREATE TABLE staging.app_categories (
    appid INTEGER,
    category_id INTEGER
);

CREATE TABLE staging.genres (
    genre_id INTEGER,
    description TEXT
);

CREATE TABLE staging.categories (
    category_id INTEGER,
    description TEXT
);

CREATE TABLE staging.app_reviews (
    appid INTEGER,
    num_reviews INTEGER,
    review_score INTEGER,
    total_positive INTEGER,
    total_negative INTEGER,
    total_reviews INTEGER
);

CREATE TABLE staging.app_players (
    appid INTEGER,
    month TEXT,
    avg_players FLOAT,
    gain FLOAT,
    percent_gain FLOAT,
    peak_players INTEGER
);


-- import csv to tables here


-- validating import

SELECT COUNT(*) FROM staging.app_details;
SELECT COUNT(*) FROM staging.app_players;
SELECT COUNT(*) FROM staging.app_reviews;

SELECT COUNT(DISTINCT appid) FROM staging.app_players;


-- setting up core tables

CREATE TABLE core.app_combined_details (
	-- app_details part
    appid INTEGER PRIMARY KEY,
    name TEXT,
    is_free BOOLEAN,
    coming_soon BOOLEAN,
    release_date DATE,
    price_usd NUMERIC(10,2),
    recommendations INTEGER,

    -- app_review part
    review_score INTEGER,
    total_positive INTEGER,
    total_negative INTEGER,
    total_reviews INTEGER,

	-- app_details remaining
    developers TEXT,
    publishers TEXT
);

CREATE TABLE core.genres (
    genre_id INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE core.categories (
    category_id INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE core.app_genre (
    appid INTEGER,
    genre_id INTEGER
);

CREATE TABLE core.app_category (
    appid INTEGER,
    category_id INTEGER
);

CREATE TABLE core.player_metrics (
    appid INTEGER,
    month DATE,
    avg_players FLOAT,
    gain FLOAT,
    percent_gain FLOAT,
    peak_players INTEGER
);


-- inserting values
INSERT INTO core.genres
SELECT * FROM staging.genres;

INSERT INTO core.categories
SELECT * FROM staging.categories;

INSERT INTO core.app_genre
SELECT * FROM staging.app_genres;

INSERT INTO core.app_category
SELECT * FROM staging.app_categories;

INSERT INTO core.fact_player_metrics
SELECT
    appid,
    TO_DATE(month, 'YYYY-MM'),
    avg_players,
    gain,
    percent_gain,
    peak_players
FROM staging.app_players;


-- function to format release date
CREATE OR REPLACE FUNCTION format_release_date(text_input TEXT)
RETURNS DATE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN
    CASE
        -- NULL or empty
        WHEN text_input IS NULL OR text_input = '' THEN NULL
        
        -- Coming soon / To be announced
        WHEN text_input ILIKE 'coming soon'
          OR text_input ILIKE 'to be announced'
        THEN NULL
        
        -- YYYY
        WHEN text_input ~ '^\d{4}$'
        THEN make_date(text_input::int, 1, 1)
        
        -- Qn YYYY
        WHEN text_input ~ '^Q[1-4]\s+\d{4}$'
        THEN make_date(
                substring(text_input from '\d{4}')::int,
                (substring(text_input from 2 for 1)::int - 1) * 3 + 1,
                1
             )
        
        -- Month YYYY
        WHEN text_input ~ '^[A-Za-z]+\s+\d{4}$'
        THEN to_date(text_input, 'Month YYYY')
        
        -- Mon DD, YYYY
        WHEN text_input ~ '^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}$'
        THEN to_date(text_input, 'Mon DD, YYYY')
        
        ELSE NULL
    END;
END;
$$;

INSERT INTO core.dim_game
SELECT
    d.appid,
    d.name,
    d.is_free,
    d.coming_soon,

    -- clean date
    format_release_date(d.release_date),

    -- convert cents → dollars
    d.price / 100.0,

    d.recommendations,

    -- review fields (nullable if missing)
    r.review_score,
    r.total_positive,
    r.total_negative,
    r.total_reviews,

    d.developers,
    d.publishers

FROM staging.app_details d
LEFT JOIN staging.app_reviews r
    ON d.appid = r.appid;


-- validating loaded date
SELECT COUNT(*) FROM core.app_combined_details;
SELECT COUNT(*) FROM core.player_metrics;

SELECT appid, COUNT(*)
FROM core.app_combined_details
GROUP BY appid
HAVING COUNT(*) > 1;


-- setting primary keys

-- remove duplicates
DELETE FROM core.player_metrics a
USING core.player_metrics b
WHERE a.ctid < b.ctid
	AND a.appid = b.appid
	AND a.month = b.month;

SELECT appid, month FROM core.player_metrics
GROUP BY appid, month
HAVING COUNT(*)>1;

ALTER TABLE core.player_metrics
ADD PRIMARY KEY (appid, month);

ALTER TABLE core.app_genre
ADD PRIMARY KEY (appid, genre_id);

ALTER TABLE core.app_category
ADD PRIMARY KEY (appid, category_id);


-- setting foreign keys
ALTER TABLE core.player_metrics
ADD CONSTRAINT fk_players_game
FOREIGN KEY (appid)
REFERENCES core.app_combined_details(appid)
ON DELETE CASCADE;

SELECT DISTINCT players.appid
FROM core.player_metrics players
LEFT JOIN core.app_combined_details details ON details.appid = players.appid
WHERE details.appid IS NULL;

-- delete orphan data on player_metrics due to local restrictions or other
SELECT DISTINCT players.appid
FROM core.player_metrics players
LEFT JOIN core.app_combined_details details ON details.appid = players.appid
WHERE details.appid IS NULL;

ALTER TABLE core.player_metrics
ADD CONSTRAINT fk_players_game
FOREIGN KEY (appid)
REFERENCES core.app_combined_details(appid)
ON DELETE CASCADE;

-- app_combined_details <-> app_genre <-> genre
ALTER TABLE core.app_genre
ADD CONSTRAINT fk_genre_game
FOREIGN KEY (appid)
REFERENCES core.app_combined_details(appid)
ON DELETE CASCADE;

ALTER TABLE core.app_genre
ADD CONSTRAINT fk_app_genre
FOREIGN KEY (genre_id)
REFERENCES core.genres(genre_id)
ON DELETE RESTRICT;

-- app_combined_details <-> app_category <-> categories
ALTER TABLE core.app_category
ADD CONSTRAINT fk_category_game
FOREIGN KEY (appid)
REFERENCES core.app_combined_details(appid)
ON DELETE CASCADE;

ALTER TABLE core.app_category
ADD CONSTRAINT fk_app_category
FOREIGN KEY (category_id)
REFERENCES core.categories(category_id)
ON DELETE RESTRICT;


-- creating indexes for queries
CREATE INDEX idx_players_appid
ON core.player_metrics(appid);

CREATE INDEX idx_genre_appid
ON core.app_genre(appid);

CREATE INDEX idx_category_appid
ON core.app_category(appid);

CREATE INDEX idx_players_month
ON core.player_metrics(month);
