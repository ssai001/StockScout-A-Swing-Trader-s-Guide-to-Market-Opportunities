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
INSERT INTO finviz_stock_screener ("Ticker", "Perf Month", "Avg Volume", "Price", "Volume")
VALUES ('CHS', '1.89%', '3.20M', 88.88, 1111111); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;


--Insert to detect change
DELETE FROM finviz_stock_screener;
INSERT INTO finviz_stock_screener ("Ticker", "Perf Month", "Avg Volume", "Price", "Volume")
VALUES ('CHS', '2.94%', '4.39M', 44.44, 2222222); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;


--Insert to detect additional changes (same as above but inserting new data)
DELETE FROM finviz_stock_screener;
INSERT INTO finviz_stock_screener ("Ticker", "Perf Month", "Avg Volume", "Price", "Volume")
VALUES ('CHS', '4.93%', '8.49M', 22.22, 3333333); --can change values every time
CALL finviz_all_list();
select * from finviz_all_list order by "Count" desc;

-----------------------------------------------------------------------------------------------


---------------------------Queries to get count of records from each table---------------------------
UPDATE finviz_all_list SET "Count" = finviz_all_list."Count" - 1, "Last_Updated_On" = '2021-07-30' 
WHERE finviz_all_list."Last_Updated_On" = '2021-08-02';


---------------------------Queries to create tables----------------------------------------------
CREATE TABLE finviz_stock_screener(
    "Ticker" VARCHAR(20), 
    "Performance_Month" VARCHAR(20), 
    "Price" DECIMAL, 
    "Average_Volume" VARCHAR(20), 
    "Volume" BIGINT
);


CREATE TABLE finviz_all_list(
    "Count" INTEGER, 
    "Ticker" VARCHAR(20), 
    "Current_Price" DECIMAL, 
    "Previous_Price" DECIMAL, 
    "Current_Volume" BIGINT,
    "Previous_Volume" BIGINT, 
    "Current_Average_Volume" VARCHAR(20),
    "Previous_Average_Volume" VARCHAR(20), 
    "Current_Performance" VARCHAR(20), 
    "Previous_Performance" VARCHAR(20), 
    "Initial_Insert" DATE, 
    "Last_Updated_On" DATE
);
-----------------------------------------------------------------------------------------------


---------------------------Queries to alter tables----------------------------------------------
ALTER TABLE finviz_all_list ADD CONSTRAINT uniqueticker UNIQUE ("Ticker");

ALTER TABLE finviz_all_list ADD COLUMN "Status" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Price_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Volume_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Average_Volume_Behavior" TEXT;
ALTER TABLE finviz_all_list ADD COLUMN "Performance_Behavior" TEXT;

ALTER TABLE finviz_all_list ALTER COLUMN "Status" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Price_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Volume_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Average_Volume_Behavior" SET NOT NULL;
ALTER TABLE finviz_all_list ALTER COLUMN "Performance_Behavior" SET NOT NULL;
-----------------------------------------------------------------------------------------------


---------------------------Queries to develop stored procedures---------------------------
CREATE OR REPLACE PROCEDURE finviz_all_list() 
AS $$
DECLARE
    REC RECORD;
BEGIN
    FOR REC in (SELECT * FROM finviz_stock_screener) 
    LOOP
        INSERT INTO finviz_all_list ("Count", "Ticker", "Current_Price", "Previous_Price",
        "Current_Volume", "Previous_Volume", "Current_Average_Volume", "Previous_Average_Volume",
        "Current_Performance", "Previous_Performance", "Initial_Insert", "Last_Updated_On",
        "Status", "Price_Behavior", "Volume_Behavior", "Average_Volume_Behavior", "Performance_Behavior")
        VALUES (1, REC."Ticker", REC."Price", REC."Price",
        REC."Volume", REC."Volume", REC."Avg Volume", REC."Avg Volume",
        REC."Perf Month", REC."Perf Month", CURRENT_DATE, CURRENT_DATE,
        'NEW INSERT','Initial Price','Initial Volume','Initial Average Volume','Initial Performance')
        ON CONFLICT ("Ticker")
        DO
            UPDATE SET "Count" = finviz_all_list."Count" + 1, 
            "Current_Price" = REC."Price", "Previous_Price" = finviz_all_list."Current_Price",
            "Current_Volume" = REC."Volume", "Previous_Volume" = finviz_all_list."Current_Volume", 
            "Current_Average_Volume" = REC."Avg Volume", "Previous_Average_Volume" = finviz_all_list."Current_Average_Volume",
            "Current_Performance" = REC."Perf Month", "Previous_Performance" = finviz_all_list."Current_Performance", 
            "Last_Updated_On" = CURRENT_DATE;
    END LOOP;
    UPDATE finviz_all_list AS fal
    SET 
    "Status" = CASE
        WHEN fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'NEW INSERT'
        WHEN fal."Count" > 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'UPDATED'
        ELSE 'ARCHIVED'
        END,
    "Price_Behavior" = CASE
        WHEN fal."Current_Price" > fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Increase From ' || fal."Previous_Price" || ' to ' || fal."Current_Price"
        WHEN fal."Current_Price" = fal."Previous_Price" AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Initial Price'
        WHEN fal."Current_Price" = fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'No Change'
        WHEN fal."Current_Price" < fal."Previous_Price" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Decrease From ' || fal."Previous_Price" || ' to ' || fal."Current_Price"
        ELSE 'Not Applicable'
        END,
    "Volume_Behavior" = CASE
        WHEN fal."Current_Volume" > fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Increase From ' || fal."Previous_Volume" || ' to ' || fal."Current_Volume"
        WHEN fal."Current_Volume" = fal."Previous_Volume" AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Initial Volume'
        WHEN fal."Current_Volume" = fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'No Change'
        WHEN fal."Current_Volume" < fal."Previous_Volume" AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Decrease From ' || fal."Previous_Volume" || ' to ' || fal."Current_Volume"
        ELSE 'Not Applicable'
        END,
    "Average_Volume_Behavior" = CASE
        WHEN LEFT(fal."Current_Average_Volume",-1)::DECIMAL > LEFT(fal."Previous_Average_Volume",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Increase From ' || fal."Previous_Average_Volume" || ' to ' || fal."Current_Average_Volume"
        WHEN LEFT(fal."Current_Average_Volume",-1)::DECIMAL = LEFT(fal."Previous_Average_Volume",-1)::DECIMAL AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Initial Average Volume'
        WHEN LEFT(fal."Current_Average_Volume",-1)::DECIMAL = LEFT(fal."Previous_Average_Volume",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'No Change'
        WHEN LEFT(fal."Current_Average_Volume",-1)::DECIMAL < LEFT(fal."Previous_Average_Volume",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Decrease From ' || fal."Previous_Average_Volume" || ' to ' || fal."Current_Average_Volume"
        ELSE 'Not Applicable'
        END,
    "Performance_Behavior" = CASE
        WHEN LEFT(fal."Current_Performance",-1)::DECIMAL > LEFT(fal."Previous_Performance",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Increase From ' || fal."Previous_Performance" || ' to ' || fal."Current_Performance"
        WHEN LEFT(fal."Current_Performance",-1)::DECIMAL = LEFT(fal."Previous_Performance",-1)::DECIMAL AND fal."Count" = 1 AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Initial Performance'
        WHEN LEFT(fal."Current_Performance",-1)::DECIMAL = LEFT(fal."Previous_Performance",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'No Change'
        WHEN LEFT(fal."Current_Performance",-1)::DECIMAL < LEFT(fal."Previous_Performance",-1)::DECIMAL AND fal."Last_Updated_On" = CURRENT_DATE THEN 'Decrease From ' || fal."Previous_Performance" || ' to ' || fal."Current_Performance"
        ELSE 'Not Applicable'
    END;
END;
$$
LANGUAGE PLPGSQL;
-------------------------------------------------------------------------------------------