---------------------------Queries to get list of records from each table---------------------------
select * from finviz_stock_screener;
select * from finviz_archive;
select * from finviz_stock_screener_unique;
select * from finviz_all_list order by "Count" desc;
----------------------------------------------------------------------------------------------------


---------------------------Queries to get count of records from each table---------------------------
select count(*) from finviz_stock_screener;
select count(*) from finviz_archive;
-----------------------------------------------------------------------------------------------------


---------------------------Queries to update records from each table (mop-up)---------------------------
UPDATE finviz_all_list SET "Count" = finviz_all_list."Count" - 1, "Last_Updated_On" = '2021-07-30' 
WHERE finviz_all_list."Last_Updated_On" = '2021-08-02';
-----------------------------------------------------------------------------------------------


---------------------------Queries to create tables----------------------------------------------
CREATE TABLE finviz_stock_screener(
    "Ticker" VARCHAR(20), 
    "Performance_Month" VARCHAR(20), 
    "Price" DECIMAL, 
    "Change" VARCHAR(20), 
    "Average_Volume" VARCHAR(20), 
    "Volume" INTEGER
);
-----------------------------------------------------------------------------------------------


---------------------------Queries to alter tables----------------------------------------------
ALTER TABLE finviz_all_list ADD CONSTRAINT uniqueticker UNIQUE ("Ticker");
-----------------------------------------------------------------------------------------------


---------------------------Queries to develop stored procedures---------------------------
CREATE OR REPLACE PROCEDURE finviz_all_list() 
AS $$
DECLARE
    REC RECORD;
BEGIN
    FOR REC in (SELECT "Ticker" FROM finviz_stock_screener_unique) 
    LOOP
        INSERT INTO finviz_all_list ("Count", "Ticker", "Last_Updated_On")
        VALUES (1, REC."Ticker", CURRENT_DATE)
        ON CONFLICT ("Ticker")
        DO
            UPDATE SET "Count" = finviz_all_list."Count" + 1, "Last_Updated_On" = CURRENT_DATE;
    END LOOP;
END;
$$
LANGUAGE PLPGSQL;
-------------------------------------------------------------------------------------------