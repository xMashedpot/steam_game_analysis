-- creating views for query

-- combining app_combined_details and player_metrics
CREATE OR REPLACE VIEW analytics.game_metrics AS
SELECT
    g.appid,
    g.name,
    g.is_free,
    g.price_usd,
    g.review_score,
    p.month,
    p.avg_players,
    p.peak_players,
    p.percent_gain
FROM core.app_combined_details g
JOIN core.player_metrics p USING (appid);

-- top average players
SELECT name, AVG(avg_players)
FROM analytics.game_metrics
GROUP BY name
ORDER BY AVG(avg_players) DESC;

-- Steam average players
SELECT
    month,
    SUM(avg_players) AS total_players,
    COUNT(DISTINCT appid) AS active_games
FROM core.player_metrics
GROUP BY month
ORDER BY month;

-- Steam average player retention by month
WITH lifecycle AS (
    SELECT
        p.appid,
        (
            DATE_PART('year', age(p.month, g.release_date)) * 12 +
            DATE_PART('month', age(p.month, g.release_date))
        ) AS months_since_release,
        p.avg_players
    FROM core.player_metrics p
    JOIN core.app_combined_details g USING (appid)
    WHERE g.release_date IS NOT NULL
)
SELECT
    months_since_release,
    AVG(avg_players) AS avg_players,
    COUNT(DISTINCT appid) AS games_sampled
FROM lifecycle
WHERE months_since_release BETWEEN 0 AND 36
GROUP BY months_since_release
HAVING COUNT(DISTINCT appid) > 25
ORDER BY months_since_release;

-- game lifetime stats
CREATE OR REPLACE VIEW analytics.game_lifecycle AS
SELECT
    appid,

    COUNT(*) AS months_tracked,

    AVG(avg_players) AS lifetime_avg_players,
    MAX(peak_players) AS lifetime_peak_players,

    STDDEV(avg_players) AS player_volatility,

    MIN(month) AS first_month,
    MAX(month) AS last_month

FROM core.player_metrics
GROUP BY appid;

-- combining 
CREATE OR REPLACE VIEW analytics.game_profile AS
SELECT
    d.appid,
    d.name,
    d.is_free,
    d.price_usd,
    d.total_positive::double precision / NULLIF(d.total_reviews, 0) AS review_ratio,

    lc.lifetime_avg_players,
    lc.lifetime_peak_players,
    lc.player_volatility,
    lc.months_tracked

FROM core.app_combined_details d
JOIN analytics.game_lifecycle lc USING (appid);

ALTER VIEW analytics.game_profile
RENAME COLUMN price_usd TO price;

-- genres and cateogries table
CREATE OR REPLACE VIEW analytics.genre_profile AS
SELECT
    gp.*,
    g.genre_id,
    ge.description AS genre
FROM analytics.game_profile gp
JOIN core.app_genre g USING (appid)
JOIN core.genres ge USING (genre_id);

ALTER VIEW analytics.genre_profile
RENAME COLUMN price_cad TO price;


CREATE OR REPLACE VIEW analytics.category_profile AS
SELECT
    gp.*,
    c.category_id,
    ca.description AS category
FROM analytics.game_profile gp
JOIN core.app_category c USING (appid)
JOIN core.categories ca ON ca.category_id = c.category_id;

ALTER VIEW analytics.category_profile
RENAME COLUMN price_cad TO price;

-- pricing bucket
CREATE OR REPLACE VIEW analytics.pricing_profile AS
SELECT *,
    CASE
        WHEN is_free THEN 'Free'
        WHEN price < 10 THEN 'Budget'
        WHEN price < 30 THEN 'Mid-Tier'
        WHEN price < 60 THEN 'Premium'
        ELSE 'High-End'
    END AS price_bucket,

    lifetime_avg_players / NULLIF(price, 0) AS engagement_per_dollar,
    lifetime_peak_players / NULLIF(price, 0) AS peak_per_dollar
FROM analytics.game_profile;


-- player count by months since release
CREATE OR REPLACE VIEW analytics.lifecycle AS
SELECT
    p.appid,
    (
        DATE_PART('year', age(p.month, g.release_date)) * 12 +
        DATE_PART('month', age(p.month, g.release_date))
    )::int AS months_since_release,
    p.avg_players
FROM core.player_metrics p
JOIN core.app_combined_details g USING (appid)
WHERE g.release_date IS NOT NULL;

-- lifecycle with genre
CREATE OR REPLACE VIEW analytics.lifecycle_with_genre AS
SELECT
    lb.appid,
    ge.description AS genre,
    lb.months_since_release,
    lb.avg_players
FROM analytics.lifecycle lb
JOIN core.app_genre ag USING (appid)
JOIN core.genres ge USING (genre_id);

-- restrict time window to 3 years
CREATE OR REPLACE VIEW analytics.lifecycle_36m AS
SELECT *
FROM analytics.lifecycle_with_genre
WHERE months_since_release BETWEEN 1 AND 36;

-- genre curves
CREATE OR REPLACE VIEW analytics.genre_lifecycle_curve AS
SELECT
    genre,
    months_since_release,
    AVG(avg_players) AS avg_players,
    COUNT(DISTINCT appid) AS games_sampled
FROM analytics.lifecycle_36m
GROUP BY genre, months_since_release
HAVING COUNT(DISTINCT appid) >= 10;

-- overall curve
CREATE OR REPLACE VIEW analytics.overall_lifecycle_curve AS
SELECT
    months_since_release,
    AVG(avg_players) AS avg_players,
    COUNT(DISTINCT appid) AS games_sampled
FROM analytics.lifecycle
WHERE months_since_release BETWEEN 1 AND 36
GROUP BY months_since_release
HAVING COUNT(DISTINCT appid) >= 25;

-- combined ovarall + genre
CREATE OR REPLACE VIEW analytics.lifecycle_chart AS
SELECT
    genre AS series,
    months_since_release,
    avg_players
FROM analytics.genre_lifecycle_curve

UNION ALL

SELECT
    'All Games' AS series,
    months_since_release,
    avg_players
FROM analytics.overall_lifecycle_curve;


-- combining lifecycle + pricing
CREATE OR REPLACE VIEW analytics.lifecycle_with_price AS
SELECT
    l.appid,
    pp.price_bucket,
    l.months_since_release,
    l.avg_players
FROM analytics.lifecycle l
JOIN analytics.pricing_profile pp USING (appid);

-- restrict time window to 3 years
CREATE OR REPLACE VIEW analytics.lifecycle_price_36m AS
SELECT *
FROM analytics.lifecycle_with_price
WHERE months_since_release BETWEEN 1 AND 36;

-- pricing curves
CREATE OR REPLACE VIEW analytics.price_lifecycle_curve AS
SELECT
    price_bucket AS series,
    months_since_release,
    AVG(avg_players) AS avg_players,
    COUNT(DISTINCT appid) AS games_sampled
FROM analytics.lifecycle_price_36m
GROUP BY price_bucket, months_since_release
HAVING COUNT(DISTINCT appid) >= 10;

-- combining overall + pricing
CREATE OR REPLACE VIEW analytics.lifecycle_price_chart AS
SELECT
    series,
    months_since_release,
    avg_players
FROM analytics.price_lifecycle_curve

UNION ALL

SELECT
    'All Games' AS series,
    months_since_release,
    avg_players
FROM analytics.overall_lifecycle_curve;


-- free vs paid ver.
CREATE OR REPLACE VIEW analytics.lifecycle_with_model AS
SELECT
    l.appid,
    CASE WHEN pp.is_free THEN 'Free' ELSE 'Paid' END AS model,
    l.months_since_release,
    l.avg_players
FROM analytics.lifecycle l
JOIN analytics.pricing_profile pp USING (appid);

-- restrict time window to 3 years
CREATE OR REPLACE VIEW analytics.lifecycle_model_36m AS
SELECT *
FROM analytics.lifecycle_with_model
WHERE months_since_release BETWEEN 1 AND 36;

-- pricing model curves
CREATE OR REPLACE VIEW analytics.model_lifecycle_curve AS
SELECT
    model AS series,
    months_since_release,
    AVG(avg_players) AS avg_players,
    COUNT(DISTINCT appid) AS games_sampled
FROM analytics.lifecycle_model_36m
GROUP BY model, months_since_release
HAVING COUNT(DISTINCT appid) >= 10;

-- combining pricing model + overall 
CREATE OR REPLACE VIEW analytics.lifecycle_model_chart AS
SELECT
    series,
    months_since_release,
    avg_players
FROM analytics.model_lifecycle_curve

UNION ALL

SELECT
    'All Games' AS series,
    months_since_release,
    avg_players
FROM analytics.overall_lifecycle_curve;


-- review-peak players scatter plot
CREATE OR REPLACE VIEW analytics.scatter_review_peak AS
SELECT
    appid,
    name,
    review_ratio,
    lifetime_peak_players
FROM analytics.game_profile
WHERE review_ratio IS NOT NULL
  AND lifetime_peak_players IS NOT NULL;
