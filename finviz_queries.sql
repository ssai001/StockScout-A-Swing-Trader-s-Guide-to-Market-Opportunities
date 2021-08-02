select count(*) from finviz_stock_screener; --index, Ticker
select * from finviz_stock_screener; --index, Ticker
select count(*) from finviz_archive;        --index, Ticker
select * from finviz_archive;        --index, Ticker


select DISTINCT "index","Ticker","Datetime" from finviz_stock_screener order by index asc; --index, Ticker

select * from finviz_stock_screener where "index" = 35; --index, Ticker



-- INSERT INTO finviz_stock_screener("Ticker")
-- VALUES (31, 'SIDDHARTH');
-- COMMIT;

DELETE FROM finviz_stock_screener;
DELETE FROM finviz_archive;
DELETE FROM finviz_stock_screener_unique;



DROP PROCEDURE finviz_archive;

CREATE OR REPLACE PROCEDURE finviz_archive() 
AS $$
BEGIN
	DELETE FROM finviz_archive;
	DELETE FROM finviz_stock_screener; --deleting data from finviz_stock_screener deletes finviz_archive data
END;
$$
LANGUAGE PLPGSQL;




CREATE OR REPLACE FUNCTION finviz_audit() RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO finviz_archive 
        SELECT DISTINCT OLD.* order by index asc;
        RETURN OLD;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER finviz_archive
AFTER DELETE ON finviz_stock_screener
    FOR EACH ROW EXECUTE PROCEDURE finviz_audit();



create table finviz_stock_screener_unique
as
select DISTINCT "index","Ticker","Datetime" 
from finviz_stock_screener order by index asc; --index, Ticker




select * from finviz_stock_screener_unique;
select * from finviz_archive;
select * from finviz_stock_screener;
select * from finviz_all_list order by "Count" desc;

UPDATE finviz_all_list SET "Count" = finviz_all_list."Count" - 1, finviz_all_list."Last_Updated_On" = "2021-07-30" WHERE finviz_all_list."Last_Updated_On" = "2021-08-02";
-- DELETE FROM finviz_all_list;
-- ALTER TABLE finviz_all_list ADD CONSTRAINT uniqueticker UNIQUE ("Ticker");





select DISTINCT "index","Ticker","Datetime" from finviz_stock_screener where "Datetime" = CURRENT_DATE order by index asc;


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


CREATE OR REPLACE FUNCTION finviz_delete_archive() RETURNS TRIGGER AS $$
    BEGIN
        DELETE FROM finviz_archive;
        DELETE FROM finviz_stock_screener;
        DELETE FROM finviz_stock_screener_unique;
    END;
$$ LANGUAGE plpgsql;



-- select customercity,customername,orderamount, avg(orderamount) over (partition by customercity)

--CREATE INDEX idx_persons on PERSONS (firstname, lastname)

--select * from categories where category_name REGEXP 'mon+';

--Important regex symbols

--[[:<:]] and [[:>:]]

--select * from customerstable where firstname REGEXP '^ba';

--ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
--ROWS BETWEEN 3 PRECEDING AND CURRENT ROW

