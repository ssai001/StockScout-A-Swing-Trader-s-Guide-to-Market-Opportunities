---------------------------Queries to get list of records from each table---------------------------
select * from finviz_stock_screener;
select * from finviz_all_list order by "Count" desc;
----------------------------------------------------------------------------------------------------


---------------------------Queries to get count of records from each table---------------------------
select count(*) from finviz_stock_screener;
-----------------------------------------------------------------------------------------------------


---------------------------Test Cases: Important Queries for Mop-up Process---------------------------

--Initial Insert
DELETE FROM finviz_stock_screener;
DELETE FROM finviz_all_list;
INSERT INTO finviz_stock_screener ("Ticker", "SMA50", "RSI", "Price", "Volume")
VALUES ('CHS', '1.89%', 34.63, 88.88, 1111111); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;


--Insert to detect change
DELETE FROM finviz_stock_screener;
INSERT INTO finviz_stock_screener ("Ticker", "SMA50", "RSI", "Price", "Volume")
VALUES ('CHS', '2.94%', 31.11, 44.44, 2222222); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;


--Insert to detect additional changes (same as above but inserting new data)
DELETE FROM finviz_stock_screener;
INSERT INTO finviz_stock_screener ("Ticker", "SMA50", "RSI", "Price", "Volume")
VALUES ('CHS', '4.93%', 40.00, 22.22, 3333333); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;

-----------------------------------------------------------------------------------------------

---------------------------Queries to create tables----------------------------------------------
CREATE TABLE finviz_stock_screener(
    "Ticker" VARCHAR(20), 
    "SMA50" VARCHAR(20), 
    "RSI" DECIMAL, 
    "Price" DECIMAL, 
    "Volume" BIGINT
);


CREATE TABLE finviz_all_list(
    "Count" INTEGER, 
    "Ticker" VARCHAR(20), 
    "Current_SMA50" VARCHAR(20), 
    "Previous_SMA50" VARCHAR(20), 
    "Current_RSI" DECIMAL,
    "Previous_RSI" DECIMAL, 
    "Current_Price" DECIMAL,
    "Previous_Price" DECIMAL, 
    "Current_Volume" BIGINT, 
    "Previous_Volume" BIGINT, 
    "Initial_Insert" DATE, 
    "Last_Updated_On" DATE
);
-----------------------------------------------------------------------------------------------


---------------------------Queries to alter tables----------------------------------------------

ALTER TABLE finviz_stock_screener DROP COLUMN "Name"; -- RUN THIS!!

ALTER TABLE finviz_stock_screener ADD COLUMN "Company" VARCHAR(100); -- RUN THIS!!
ALTER TABLE finviz_stock_screener ADD COLUMN "URL" VARCHAR(50);
ALTER TABLE finviz_stock_screener ADD COLUMN "Sector" VARCHAR(50);
ALTER TABLE finviz_stock_screener ADD COLUMN "Industry" VARCHAR(50);
ALTER TABLE finviz_stock_screener ALTER COLUMN "Company" SET NOT NULL; -- RUN THIS!!
ALTER TABLE finviz_stock_screener ALTER COLUMN "URL" SET NOT NULL;
ALTER TABLE finviz_stock_screener ALTER COLUMN "Sector" SET NOT NULL;
ALTER TABLE finviz_stock_screener ALTER COLUMN "Industry" SET NOT NULL;


ALTER TABLE finviz_all_list DROP COLUMN "Name"; -- RUN THIS!!


ALTER TABLE finviz_all_list ADD CONSTRAINT uniqueticker UNIQUE ("Ticker");
ALTER TABLE finviz_all_list ADD COLUMN "Status" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "SMA50_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "RSI_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Price_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Volume_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Company" VARCHAR(100); -- RUN THIS!!
ALTER TABLE finviz_all_list ADD COLUMN "URL" VARCHAR(50);
ALTER TABLE finviz_all_list ADD COLUMN "Sector" VARCHAR(50);
ALTER TABLE finviz_all_list ADD COLUMN "Industry" VARCHAR(50);
ALTER TABLE finviz_all_list ALTER COLUMN "Status" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "SMA50_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "RSI_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Price_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Volume_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Company" SET NOT NULL; -- RUN THIS!!
ALTER TABLE finviz_all_list ALTER COLUMN "URL" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Sector" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Industry" SET NOT NULL;
-----------------------------------------------------------------------------------------------


---------------------------Queries to develop stored procedures---------------------------
CREATE OR REPLACE PROCEDURE finviz_all_list() 
AS $$
DECLARE
    REC RECORD;
    CURRENT_DATE_EST DATE;
BEGIN
    CURRENT_DATE_EST := TO_DATE(TO_CHAR(NOW()::DATE at time zone 'America/New_York', 'YYYY-MM-DD'),'YYYY-MM-DD');
    FOR REC in (SELECT * FROM finviz_stock_screener) 
    LOOP
        INSERT INTO finviz_all_list ("Count", "Ticker", "Current_SMA50", "Previous_SMA50",
        "Current_RSI", "Previous_RSI", "Current_Price", "Previous_Price",
        "Current_Volume", "Previous_Volume", "Initial_Insert", "Last_Updated_On",
        "Status", "SMA50_Behavior", "RSI_Behavior", "Price_Behavior", "Volume_Behavior",
        "Company", "URL", "Sector", "Industry")
        VALUES (1, REC."Ticker", REC."SMA50", REC."SMA50",
        REC."RSI", REC."RSI", REC."Price", REC."Price",
        REC."Volume", REC."Volume", CURRENT_DATE_EST, CURRENT_DATE_EST,
        'NEW INSERT','Initial SMA50','Initial RSI','Initial Price','Initial Volume',
        REC."Company", REC."URL", REC."Sector", REC."Industry")
        ON CONFLICT ("Ticker") WHERE finviz_all_list."Last_Updated_On" != CURRENT_DATE_EST
        DO
            UPDATE SET "Count" = finviz_all_list."Count" + 1, 
            "Current_SMA50" = REC."SMA50", "Previous_SMA50" = finviz_all_list."Current_SMA50",
            "Current_RSI" = REC."RSI", "Previous_RSI" = finviz_all_list."Current_RSI", 
            "Current_Price" = REC."Price", "Previous_Price" = finviz_all_list."Current_Price",
            "Current_Volume" = REC."Volume", "Previous_Volume" = finviz_all_list."Current_Volume", 
            "Last_Updated_On" = CURRENT_DATE_EST;
    END LOOP;
    UPDATE finviz_all_list AS fal
    SET 
    "Status" = CASE
        WHEN fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'NEW INSERT'
        WHEN fal."Count" > 1 AND LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL
        AND fal."Current_RSI" = fal."Previous_RSI" 
        AND fal."Current_Price" = fal."Previous_Price" 
        AND fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'NEW INSERT'
        WHEN fal."Count" > 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'UPDATED'
        ELSE 'ARCHIVED'
        END,
    "SMA50_Behavior" = CASE
        WHEN LEFT(fal."Current_SMA50",-1)::DECIMAL > LEFT(fal."Previous_SMA50",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Increase From ' || fal."Previous_SMA50" || ' to ' || fal."Current_SMA50"
        WHEN LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial SMA50 of ' || fal."Current_SMA50"
        WHEN fal."Count" > 1 AND LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL 
        AND fal."Current_RSI" = fal."Previous_RSI" 
        AND fal."Current_Price" = fal."Previous_Price" 
        AND fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial SMA50 of ' || fal."Current_SMA50"
        WHEN LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'No Change From ' || fal."Previous_SMA50"
        WHEN LEFT(fal."Current_SMA50",-1)::DECIMAL < LEFT(fal."Previous_SMA50",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Decrease From ' || fal."Previous_SMA50" || ' to ' || fal."Current_SMA50"
        ELSE 'Not Applicable'
        END,
    "RSI_Behavior" = CASE
        WHEN fal."Current_RSI" > fal."Previous_RSI" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Increase From ' || fal."Previous_RSI" || ' to ' || fal."Current_RSI"
        WHEN fal."Current_RSI" = fal."Previous_RSI" AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial RSI of ' || fal."Current_RSI"
        WHEN fal."Count" > 1 AND LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL 
        AND fal."Current_RSI" = fal."Previous_RSI" 
        AND fal."Current_Price" = fal."Previous_Price" 
        AND fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial RSI of ' || fal."Current_RSI"
        WHEN fal."Current_RSI" = fal."Previous_RSI" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'No Change From ' || fal."Previous_RSI"
        WHEN fal."Current_RSI" < fal."Previous_RSI" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Decrease From ' || fal."Previous_RSI" || ' to ' || fal."Current_RSI"
        ELSE 'Not Applicable'
        END,
    "Price_Behavior" = CASE
        WHEN fal."Current_Price" > fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Increase From ' || fal."Previous_Price" || ' to ' || fal."Current_Price"
        WHEN fal."Current_Price" = fal."Previous_Price" AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial Price of ' || fal."Current_Price"
        WHEN fal."Count" > 1 AND LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL 
        AND fal."Current_RSI" = fal."Previous_RSI" 
        AND fal."Current_Price" = fal."Previous_Price" 
        AND fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial Price of ' || fal."Current_Price"
        WHEN fal."Current_Price" = fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'No Change From ' || fal."Previous_Price"
        WHEN fal."Current_Price" < fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Decrease From ' || fal."Previous_Price" || ' to ' || fal."Current_Price"
        ELSE 'Not Applicable'
        END,
    "Volume_Behavior" = CASE
        WHEN fal."Current_Volume" > "Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Increase From ' || fal."Previous_Volume" || ' to ' || fal."Current_Volume"
        WHEN fal."Current_Volume" = fal."Previous_Volume" AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial Volume of ' || fal."Current_Volume"
        WHEN fal."Count" > 1 AND LEFT(fal."Current_SMA50",-1)::DECIMAL = LEFT(fal."Previous_SMA50",-1)::DECIMAL 
        AND fal."Current_RSI" = fal."Previous_RSI" 
        AND fal."Current_Price" = fal."Previous_Price" 
        AND fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Initial Volume of ' || fal."Current_Volume"
        WHEN "Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'No Change From ' || fal."Previous_Volume"
        WHEN "Current_Volume" < fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE_EST THEN 'Decrease From ' || fal."Previous_Volume" || ' to ' || fal."Current_Volume"
        ELSE 'Not Applicable'
    END;
END;
$$
LANGUAGE PLPGSQL;
-------------------------------------------------------------------------------------------