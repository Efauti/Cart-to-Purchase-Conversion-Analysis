CREATE DATABASE cosmetics_events_history;
USE cosmetics_events_history;

-- ==========================================================
-- Creating the 5 temporary tables
-- ==========================================================
-- Staging table used for raw load of February 2020 dataset.
-- Columns defined with maximum safe sizes (VARCHAR(255)) to avoid truncation
-- since source file is large and field lengths were uncertain.
-- Will refine to exact limits in structured tables later.

CREATE TABLE temp_feb_2020 (
    event_time DATETIME,
    event_type VARCHAR(255),
    product_id BIGINT,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(12,2),
    user_id BIGINT,
    session_id VARCHAR(255));

-- no index to allow for faster loading

-- ==========================================================
-- Loading data into the five temporary tables
-- ==========================================================
-- Bulk load raw CSV into staging table.
-- 'enclosed by' handles quoted values; 'ignore 1 rows' skips header line.
-- Nulls/empty values left as-is and cleaned downstream.
-- ENUM normalization deferred until structured table stage.

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/2020-feb.csv'
INTO TABLE temp_feb_2020
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @event_time_var,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    session_id
)
SET
    event_time = STR_TO_DATE(@event_time_var, '%Y-%m-%d %H:%i:%s');
/*
events started at exactly 12 am, STR_TO_DATE was used to handle 0000-00-00 00:00:00 time
*/

-- ==========================================================
-- Dropping incomplete column
-- ==========================================================
-- Dropped column from all five temporary tables after inspection:
-- <1% populated, no reliable relationship to product_id or 
-- category_id. Retaining would add noise without value.

ALTER TABLE temp_feb_2020
DROP COLUMN category_code;

-- ==========================================================
-- Renaming event_type values for clarity
-- ==========================================================
-- Normalize event_type values to match controlled vocabulary used in analysis.
-- Raw values found in dataset: 'view', 'remove_from_cart', 'purchase'
-- Converted to standardized labels: 'viewed', 'removed', 'purchased'
-- Ensures consistency with ENUM definition in final structured table.
-- The 'cart' events type was left as is.
UPDATE temp_feb_2020
SET event_type = 'removed' WHERE event_type = 'remove_from_cart';
UPDATE temp_feb_2020
SET event_type = 'purchased' WHERE event_type = 'purchase';
UPDATE temp_feb_2020
SET event_type = 'viewed' WHERE event_type = 'view';

select event_type, count(event_type) from temp_feb_2020 group by event_type;
-- This value was compared to the total rows in the table
-- to check for completeness

-- ==========================================================
-- Handling empty and null records
-- ==========================================================
-- event_time: column checked, no missing values detected
-- event_type: column checked, no missing values detected
-- product_id: column checked, no missing values detected
-- category_id: column checked, no missing values detected
-- user_id: column checked, no missing values detected

-- Brand: replace empty or NULL values with placeholder
UPDATE temp_feb_2020
SET brand = 'No Brand'
WHERE brand IS NULL OR brand = '';

-- User session: replace empty or NULL values with placeholder
-- NOTE: using 'unknown' may group unrelated events into one artificial session
UPDATE temp_feb_2020
SET session_id = 'unknown'
WHERE session_id IS NULL OR session_id = '';

-- Price: replace empty or NULL values with 0.00
-- All zero or negative prices will later be isolated in anomalies tables
UPDATE temp_feb_2020
SET price = 0.00
WHERE price IS NULL OR price = '';

-- ==========================================================
-- Backup (master) table creation
-- ==========================================================
-- Create a master copy of the full dataset.
-- This table will remain untouched and serve as the reference
-- point for any subsets.
-- Reason: ensures we always have a clean rollback source even
-- if working tables are modified or truncated during analysis.
-- NOTE: large table (20M+ rows). To improve insert performance,
-- indexes (beyond the primary key) were added only after the
-- data was fully loaded.

CREATE TABLE backup_events_history (
  Event_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Event_time DATETIME NOT NULL,
  Event_type ENUM('viewed','cart','removed','purchased') DEFAULT NULL,
  Category_id BIGINT DEFAULT NULL,
  Brand VARCHAR(13) DEFAULT NULL,
  Price DECIMAL(12,2) DEFAULT NULL,
  User_id BIGINT NOT NULL,
  session_id VARCHAR(36) DEFAULT NULL,
  product_id BIGINT,
-- Unique constraint ensures event-level deduplication
  CONSTRAINT unique_event UNIQUE (Event_time, Event_type, User_id, product_id, session_id)
) ENGINE=InnoDB AUTO_INCREMENT=1;

-- ==========================================================
-- Populating the backup table from staging (temp) tables
-- ==========================================================
-- Data was loaded from five monthly staging tables 
-- (October 2019 through February 2020).
-- Example shown below for February 2020; the same structure 
-- was repeated for the other months.
INSERT IGNORE INTO backup_events_history (
    Event_time, Event_type, Category_id, Brand, Product_id, Price, User_id, session_id
)
SELECT
    Event_time, Event_type, Category_id, Brand, Product_id, Price, User_id, session_id
FROM temp_feb_2020;

-- ==========================================================
-- Adding indexes (post-load)
-- ==========================================================
-- Indexes were added only after the data load to prevent 
-- performance issues during bulk inserts.
ALTER TABLE backup_events_history
  ADD INDEX event_type_idx (event_type),
  ADD INDEX brand_idx (brand),
  ADD INDEX session_idx (session_id),
  ADD INDEX funnel_idx (event_time, session_id),
  ADD INDEX category_idx (category_id),
  ADD INDEX product_idx (product_id);


-- ==========================================================
-- Events history table creation (normalized)
-- ==========================================================
-- This is the working subset of the backup (master) table.
-- It contains only the event facts; descriptive attributes 
-- such as category, brand, and price are stored separately 
-- in the products table, linked by product_id.

CREATE TABLE events_history AS
SELECT 
    Event_id,
    Event_time,
    Event_type,
    User_id,
    session_id,
    Product_id
FROM backup_events_history;

-- ==========================================================
-- Adding indexes to the events_history table
-- ==========================================================
ALTER TABLE events_history
  ADD PRIMARY KEY (event_id),
  ADD INDEX event_type_idx (event_type),
  ADD INDEX user_idx (user_id),
  ADD INDEX session_idx (session_id),
  ADD INDEX product_idx (product_id);

-- ==========================================================
-- creating the price anomalies table
-- ==========================================================
-- This table captures all events where the product price
-- was zero or negative. These records can be investigated
-- separately for data quality or business rule violations.

CREATE TABLE anomalies_price (
    Event_id BIGINT NOT NULL AUTO_INCREMENT,
    Event_time DATETIME NOT NULL,
    Event_type ENUM('viewed','cart','removed','purchased') DEFAULT NULL,
    Category_id BIGINT DEFAULT NULL,
    Brand VARCHAR(13) DEFAULT NULL,
    Price DECIMAL(12,2) DEFAULT NULL,
    User_id BIGINT NOT NULL,
    session_id VARCHAR(36) DEFAULT NULL,
    Product_id BIGINT,
    PRIMARY KEY (Event_id),
    CONSTRAINT unique_event UNIQUE (Event_time, Event_type, User_id, Product_id, session_id)
) ENGINE=InnoDB AUTO_INCREMENT=1;

INSERT IGNORE INTO anomalies_price (
	Event_time, Event_type, Category_id, Brand, Product_id, Price, User_id, session_id)
SELECT
    Event_time, Event_type, Category_id, Brand, product_id, Price, User_id, session_id
FROM  backup_events_history where price <= 0;

-- ==========================================================
-- Creating the session history table
-- ==========================================================
-- This table summarizes user sessions by capturing start, end,
-- duration, and time attributes. Temporary staging is used to
-- compute session_start and session_end efficiently.
-- indexes were added after the data was loaded to improve
-- performance

CREATE TABLE session_history (
  Session_id VARCHAR(36) NOT NULL,
  User_id BIGINT NOT NULL,
  Session_start DATETIME NULL,
  Session_end DATETIME NULL,
  Duration BIGINT NULL,
  Weekday INT NULL,
  Month INT NULL,
  Session_hour INT NULL,
  Week INT NULL
) ENGINE=InnoDB;

-- Creating a temporary table for computing session start and end
CREATE TEMPORARY TABLE temp_session_history (
Event_time DATETIME,
Session_id VARCHAR(36) NOT NULL,
User_id BIGINT NOT NULL
) ENGINE=InnoDB;

-- Populating temporary session table from events_history
INSERT INTO temp_session_history (Session_id, User_id, Event_time)
SELECT Session_id, User_id, Event_time
FROM events_history;

-- Populating session_history with aggregated session data
INSERT INTO session_history (Session_id, User_id, Session_start, Session_end)
SELECT
Session_id,
User_id,
MIN(Event_time),
MAX(Event_time)
FROM temp_session_history
GROUP BY Session_id, User_id;

-- Computing additional session metrics
START TRANSACTION;

UPDATE session_history
SET
  Duration = TIMESTAMPDIFF(MINUTE, Session_start, Session_end),
  Weekday = WEEKDAY(Session_start),
  Month = MONTH(Session_start),
  Session_hour = HOUR(Session_start),
  Week = WEEK(Session_start);
COMMIT;

-- Adding indexes for efficient filtering
ALTER TABLE session_history
  ADD INDEX idx_session_id (Session_id),
  ADD INDEX idx_user_id (User_id),
  ADD INDEX idx_session_hour (Session_hour),
  ADD INDEX idx_weekday (Weekday),
  ADD INDEX idx_month (Month),
  ADD INDEX idx_week (Week);

-- Drop temporary table after use
DROP TEMPORARY TABLE temp_session_history;

-- ==========================================================
-- Handling anomalous sessions (multiple users sharing same session_id)  
-- ==========================================================  
-- Identifying session_ids associated with more than one distinct user
CREATE TEMPORARY TABLE anomalous_sessions AS
SELECT session_id
FROM session_history
GROUP BY session_id
HAVING COUNT(DISTINCT user_id) > 1;

-- Records with unknown sessions were also included, as
-- they will aggregate into one very large session which 
-- might bias analysis.

-- Creating a table to store these anomalous session events  
CREATE TABLE session_anomalies AS
SELECT *
FROM backup_events_history
WHERE Session_id IN (SELECT session_id FROM anomalous_sessions);

-- Removing anomalous sessions from working tables  
DELETE FROM events_history
WHERE session_id IN (SELECT session_id FROM anomalous_sessions);

DELETE FROM session_history
WHERE session_id IN (SELECT session_id FROM anomalous_sessions);

-- Drop the temporary table
DROP TEMPORARY TABLE anomalous_sessions;

-- ==========================================================
-- Handling session length anomalies
-- ==========================================================
-- Identified sessions longer than 24 hours (1440 minutes)  
CREATE TEMPORARY TABLE session_length_anomalies AS
SELECT *
FROM session_history
WHERE duration > 1440;

-- Creating a table to store these anomalous session events  
CREATE TABLE Anomalies_session_length AS
SELECT *
FROM backup_events_history
WHERE Session_id IN (SELECT Session_id FROM session_length_anomalies);

-- Removing these anomalous sessions from working tables 
DELETE FROM session_history
WHERE Session_id IN (SELECT Session_id FROM session_length_anomalies);

-- ==========================================================
-- Session summary table
-- ==========================================================
-- Summarizes counts of each event type per session.

CREATE TABLE session_summary (
  session_id VARCHAR(36) NOT NULL,
  viewed INT DEFAULT 0,
  carted INT DEFAULT 0,
  purchased INT DEFAULT 0,
  removed INT DEFAULT 0,
  PRIMARY KEY (session_id)
) ENGINE=InnoDB;

-- Populating the session_summary table
-- Data inserted in batches for performance.
-- Subsequent batches continue from the last session_id inserted.
INSERT IGNORE INTO session_summary (session_id, viewed, carted, purchased, removed)
SELECT 
    session_id,
    SUM(CASE WHEN event_type = 'viewed' THEN 1 ELSE 0 END) AS viewed,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carted,
    SUM(CASE WHEN event_type = 'purchased' THEN 1 ELSE 0 END) AS purchased,
    SUM(CASE WHEN event_type = 'removed' THEN 1 ELSE 0 END) AS removed
FROM events_history
WHERE session_id > '1c74c67a-befd-4f57-a1a1-b3565c964ce5'  -- filter applied based on dataset check
GROUP BY session_id
ORDER BY session_id ASC
LIMIT 500000;

-- ==========================================================
-- Creating the products table
-- ==========================================================
-- Stores descriptive attributes for each product and preserves all observed prices.
-- Each row represents a unique combination of product_id, price, category, and brand.
-- This is intentional because all price points are important for downstream analysis

-- Creating a temporary table to hold all product info
CREATE TABLE temp_products AS
SELECT product_id, price, category_id, brand
FROM backup_events_history;

-- Creating the normalized products table with unique product entries
CREATE TABLE products AS
SELECT DISTINCT product_id, category_id, price, brand
FROM temp_products;

-- ==========================================================
-- Creating a table with the price variations for each product
-- ==========================================================
-- Captures min, max, and percentage variation per product_id
-- All zero prices has been removed in an earlier step.
CREATE TABLE product_price_variations AS
SELECT
    product_id,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND((MAX(price) - MIN(price)) / MIN(price) * 100, 2) AS price_variation
FROM
    products
GROUP BY
    product_id;

-- Drop the temporary table
DROP TABLE temp_products;
