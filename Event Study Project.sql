# AIM OF PROJECT is to analyse the effects of lockdown announcements on stock prices within the S&P 500 and FTSE 350
# The top 250 stocks (based on the top risers of 2019) from each index will be chosen
# Abnormal Return (AR) and Cumulative Abnormal Return (CAR) calculations will be used
# Average Abnormal Return (AAR) and Cumulative Average Abnormal Returns (CAAR) calculations will also be used
# Where AR = Expected return (ER) - Actual Return
# Where CAAR is the cumilative addition of AAR
# EVENT DATE = WHO COVID 19 State of Emergency Declaration: 11/03/2020
# An event study of (-20,+20), (-20,-1), (-1,+1) and (+1,+20) will be used where t=0 is the event date
# The event study stored procedure will give you the AAR and CAAR per day per index. This averages out the tickers Abnormal returns
# The trading summary stored procedure will give you the AR per day per ticker per index.
# You must use the correct stored procedure when choosing n days before and n days after the event date
# Visit my github to view the link to the event study dashboard

CREATE DATABASE IF NOT EXISTS Event_Study_Project;
USE Event_Study_Project;

#Company Info Table
#Import FTSE 350 company info.csv AND S and P company info
CREATE TABLE IF NOT EXISTS company_info (
    ticker varchar(20),
    company_name VARCHAR(255),
    industry CHAR(255),
    index_ID CHAR(2),
    PRIMARY KEY (ticker)
);

#Index Prices
#Import FTSE 350 index prices.csv AND S and P index prices.csv
CREATE TABLE IF NOT EXISTS index_prices (
    ticker CHAR(10),
    price_date DATE,
    open_price DECIMAL(10 , 2 ),
    high_price DECIMAL(10 , 2 ),
    low_price DECIMAL(10 , 2 ),
    close_price DECIMAL(10 , 2 ),
    adj_close DECIMAL(10 , 2 ),
    volume BIGINT,
    index_ID CHAR(2),
    currency CHAR(3)
);

#Constituents Prices
#Import FTSE constituents prices.csv AND S and P index prices.csv
CREATE TABLE IF NOT EXISTS stock_prices (
    ticker CHAR(10),
    price_date DATE,
    open_price DECIMAL(10 , 2 ),
    high_price DECIMAL(10 , 2 ),
    low_price DECIMAL(10 , 2 ),
    close_price DECIMAL(10 , 2 ),
    adj_close DECIMAL(10 , 2 ),
    volume BIGINT,
    index_ID CHAR(2),
    currency CHAR(3)
);

#Top 250 risers 2019
#Import FTSE top 250 risers.csv AND S and P top 250 risers.csv
CREATE TABLE IF NOT EXISTS top_risers_2019 (
    ticker VARCHAR(20),
    Jan_Price DECIMAL(10 , 2 ),
    Dec_Price DECIMAL(10 , 2 ),
    Percentage_change DECIMAL(10 , 2 ),
    Index_ID CHAR(2),
    PRIMARY KEY (ticker)
);

CREATE INDEX stock_prices
ON stock_prices (ticker, adj_close, price_date, index_ID, currency);

CREATE INDEX index_prices
ON index_prices (ticker, adj_close, price_date, index_ID, currency);

CREATE INDEX tr2019
ON top_risers_2019 (ticker, index_ID);

#Event_study stored procedure for 1 date BEFORE the event date and 1 day AFTER the event date.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `event_study`(IN p_days_before INT, IN p_event_date DATE, IN p_days_after INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v_UK_date_before := (SELECT 
       A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v_UK_date_after := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_US_date_before := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v_US_date_after := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_UK_day_count := -p_days_before-1;
SET @v_US_day_count := -p_days_before-1;

WITH CTE_UK AS (SELECT
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_UK_date_before AND @v_UK_date_after
        AND tr.index_ID = 'UK') AS A),
CTE_US AS (SELECT 
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_US_date_before AND @v_US_date_after
        AND tr.index_ID = 'US') AS A)
        
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_UK_day_count := @v_UK_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_UK AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date)
UNION
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_US_day_count := @v_US_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_US AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$

#Event_study stored procedure for 2 dates BEFORE the event period.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `event_study_negative_days`(IN p_days_before_1 INT, IN p_event_date DATE, IN p_days_before_2 INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v_UK_date_before_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before_1) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v_UK_date_before_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before_2) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v_US_date_before_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before_1) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v_US_date_before_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before_2) AS A
ORDER BY a.price_date ASc
LIMIT 1);
SET @v_UK_day_count := -p_days_before_1-1;
SET @v_US_day_count := -p_days_before_1-1;

WITH CTE_UK AS (SELECT
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_UK_date_before_1 AND @v_UK_date_before_2
        AND tr.index_ID = 'UK') AS A),
CTE_US AS (SELECT 
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_US_date_before_1 AND @v_US_date_before_2
        AND tr.index_ID = 'US') AS A)
        
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_UK_day_count := @v_UK_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_UK AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date)
UNION
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_US_day_count := @v_US_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_US AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$

#Event study stored procedure for 2 dates AFTER the event date.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `event_study_positive_days`(IN p_days_after_1 INT, IN p_event_date DATE, IN p_days_after_2 INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v_UK_date_after_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after_1) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_UK_date_after_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after_2) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_US_date_after_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after_1) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_US_date_after_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after_2) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v_UK_day_count := p_days_after_1-1;
SET @v_US_day_count := p_days_after_1-1;

WITH CTE_UK AS (SELECT
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_UK_date_after_1 AND @v_UK_date_after_2
        AND tr.index_ID = 'UK') AS A),
CTE_US AS (SELECT 
        A.*,
            (((100 / A.prev_price) * A.stock_price) - 100) AS 'percent_return',
            (((100 / A.prev_index_value) * A.index_price) - 100) AS 'expected_return'
    FROM
        (SELECT 
        tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
        AND ipa.price_date = spa.price_date
    WHERE
        spa.price_date BETWEEN @v_US_date_after_1 AND @v_US_date_after_2
        AND tr.index_ID = 'US') AS A)
        
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_UK_day_count := @v_UK_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_UK AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date)
UNION
(SELECT C.price_date, 
C.AAR, 
SUM(C.AAR) OVER (ORDER BY C.price_date) as CAAR,
@v_US_day_count := @v_US_day_count + 1  as event_day,
C.index_ID
FROM
(SELECT 
    B.price_date,
    ROUND(AVG(B.percent_return), 2) AS 'AAR',
    B.index_ID
FROM
    CTE_US AS B
GROUP BY B.price_date
ORDER BY B.price_date DESC) AS C
ORDER BY C.price_date);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$

#Trading Summary stored procedure for 1 date BEFORE and 1 date AFTER the event date.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `trading_summary`(IN p_days_before INT, IN p_event_date DATE, IN p_days_after INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v1_UK_date_before := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v1_UK_date_after := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v1_US_date_before := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v1_US_date_after := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after) AS A
ORDER BY a.price_date DESC
LIMIT 1);

(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_UK_date_before AND @v1_UK_date_after
    AND index_ID = 'UK'
ORDER BY C.price_date , C.index_ID , C.ticker)
UNION
(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_US_date_before AND @v1_US_date_after
    AND index_ID = 'US'
ORDER BY C.price_date , C.index_ID , C.ticker);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$

#Trading Summary stored procedure for 2 dates BEFORE the event date.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `trading_summary_negative_days`(IN p_days_before_1 INT, IN p_event_date DATE, IN p_days_before_2 INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v1_UK_date_before_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before_1) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v1_UK_date_before_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date DESC
    LIMIT p_days_before_2) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v1_US_date_before_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before_1) AS A
ORDER BY a.price_date ASC
LIMIT 1);
SET @v1_US_date_before_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date < p_event_date
        AND index_ID = 'US'
    ORDER BY price_date DESC
    LIMIT p_days_before_2) AS A
ORDER BY a.price_date ASC
LIMIT 1);

(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_UK_date_before_1 AND @v1_UK_date_before_2
    AND index_ID = 'UK'
ORDER BY C.price_date , C.index_ID , C.ticker)
UNION
(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_US_date_before_1 AND @v1_US_date_before_2
    AND index_ID = 'US'
ORDER BY C.price_date , C.index_ID , C.ticker);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$

#Trading Summary stored procedure for 2 dates AFTER the event date.
DELIMITER $$
USE `event_study_project`$$
CREATE PROCEDURE `trading_summary_positive_days`(IN p_days_after_1 INT, IN p_event_date DATE, IN p_days_after_2 INT)
IF (SELECT DISTINCT
                price_date
            FROM
                stock_prices
            WHERE
                price_date = p_event_date)
THEN
BEGIN
SET @v1_UK_date_after_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after_1) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v1_UK_date_after_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'UK'
    ORDER BY price_date ASC
    LIMIT p_days_after_2) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v1_US_date_after_1 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after_1) AS A
ORDER BY a.price_date DESC
LIMIT 1);
SET @v1_US_date_after_2 := (SELECT 
    A.price_date
FROM
    (SELECT DISTINCT
        price_date
    FROM
        index_prices
    WHERE
        price_date > p_event_date
        AND index_ID = 'US'
    ORDER BY price_date ASC
    LIMIT p_days_after_2) AS A
ORDER BY a.price_date DESC
LIMIT 1);

(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_UK_date_after_1 AND @v1_UK_date_after_2
    AND index_ID = 'UK'
ORDER BY C.price_date , C.index_ID , C.ticker)
UNION
(SELECT 
    C.price_date,
    C.ticker,
    C.company_name,
    C.industry,
    C.percent_return,
    C.expected_return,
    (C.percent_return - C.expected_return) AS Abnormal_return,
    CASE
        WHEN C.percent_return > expected_return THEN 'abnormal'
        ELSE 'normal'
    END AS 'abnormal/normal',
    CASE
        WHEN C.percent_return > 0 THEN 'gain'
        ELSE 'loss'
    END AS 'Gain/Loss',
    C.index_ID
FROM
    (SELECT 
        A.*,
            ROUND((((100 / A.prev_price) * A.stock_price) - 100), 2) AS 'percent_return',
            ROUND((((100 / A.prev_index_value) * A.index_price) - 100), 2) AS 'expected_return'
    FROM
        (SELECT 
        ci.company_name,
            ci.industry,
            tr.ticker,
            tr.index_ID,
            spa.price_date,
            spa.adj_close AS stock_price,
            ipa.adj_close AS index_price,
            (SELECT 
                    spb.adj_close
                FROM
                    stock_prices spb
                WHERE
                    spb.ticker = spa.ticker
                        AND spb.price_date < spa.price_date
                ORDER BY spb.price_date DESC
                LIMIT 1) AS prev_price,
            (SELECT 
                    ipb.adj_close
                FROM
                    index_prices ipb
                WHERE
                    ipb.price_date < ipa.price_date
                        AND ipb.index_ID = ipa.index_ID
                ORDER BY ipb.price_date DESC
                LIMIT 1) AS prev_index_value
    FROM
        stock_prices spa
    JOIN index_prices ipa ON ipa.index_ID = spa.index_ID
    JOIN top_risers_2019 tr ON tr.ticker = spa.ticker
    JOIN company_info ci ON ci.ticker = tr.ticker
        AND ipa.price_date = spa.price_date) AS A) AS C
WHERE
    C.price_date BETWEEN @v1_US_date_after_1 AND @v1_US_date_after_2
    AND index_ID = 'US'
ORDER BY C.price_date , C.index_ID , C.ticker);
END;
ELSE BEGIN SELECT 'event date does not exist' as ' ';
END;
END IF$$