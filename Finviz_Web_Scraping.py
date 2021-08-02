from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
import requests
import pandas as pd
import smtplib
import sqlalchemy


def main():
    req1 = "https://finviz.com/screener.ashx?v=141&f=fa_epsqoq_pos,fa_salesqoq_pos,sh_curvol_o1000,ta_beta_o1,ta_highlow20d_b5h,ta_highlow52w_a70h,ta_sma20_sa50,ta_sma200_sb50,ta_sma50_pa&ft=4&o=-perf13w"
    req2 = "https://finviz.com/screener.ashx?v=111&f=sh_avgvol_o500,sh_price_u40,sh_relvol_o0.75,ta_pattern_tlsupport2&ft=4&o=-volume"
    TickerDetection(req1,req2)


def TickerDetection(*request_url):
    
    #Initial Load of the Data
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    appended_data = []
    for i in range(0,100):
        if i % 20 == 0:
            screen = requests.get(request_url+"&r="+str(i+1), headers = headers).text
            tables = pd.read_html(screen)
            tables = tables[-2]
            tables.columns = tables.iloc[0]
            tables = tables[1:]
            appended_data.append(tables)
            appended_data_pd = pd.concat(appended_data)
            appended_data_pd = appended_data_pd.reset_index(drop=True)
            appended_data_pd = appended_data_pd.drop_duplicates()
            appended_data_pd_single_col = appended_data_pd['Ticker']
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:sidd1968!@@127.0.0.1:5432/postgres")
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute("CALL finviz_delete_archive();")
    appended_data_pd_single_col.to_sql('finviz_stock_screener', engine, if_exists='append')
    engine.execute('INSERT INTO "finviz_stock_screener_unique" select DISTINCT "index","Ticker","Datetime" from finviz_stock_screener where "Datetime" = CURRENT_DATE order by index asc')
    cursor.execute("CALL finviz_all_list();")
    connection.commit()
    cursor.close()
    connection.close()

    #Generate old and new data in list format
    old_result = engine.execute('select "Ticker" from finviz_archive')
    new_result = engine.execute('select "Ticker" from finviz_stock_screener_unique')
    #all_list_result = engine.execute('select * from finviz_all_list')
    engine.dispose()
    old_tickers = [row[0] for row in old_result]
    new_tickers = [row[0] for row in new_result]
    old_tickers_distinct = list(set(old_tickers))
    new_tickers_distinct = list(set(new_tickers))
    deleted_tickers = [x for x in old_tickers_distinct if x not in new_tickers_distinct]
    inserted_tickers = [x for x in new_tickers_distinct if x not in old_tickers_distinct]
    SendEmail(deleted_tickers,inserted_tickers,datetime.date.today())


def SendEmail(old,new,date):
    
    #Initialize Key Details
    email_sender_account = "chidachais@gmail.com"
    email_sender_username = "chidachais@gmail.com"
    email_sender_password = "Apple07101968!"
    email_smtp_server = "smtp.gmail.com"
    email_smtp_port = 587

    #Email Header
    email_recepients = ["siddharthsai@supplychaininc.com"]
    email_subject = f"Finviz Stock Tracker for {date}"
    email_body = '<html><head></head><body>'
    email_body += '<style type="text/css"></style>' 
    email_body += f'<h2>Finviz Stock Tracker for {date}</h2>' 
    
    #Tickers Deleted List
    email_body += f'<h1 style="color: rgb(86, 0, 251);">' 
    email_body += f'<b>Deleted Tickers</b>: ' 
    email_body += f'{old}</h1>' 

    #Tickers Added List
    email_body += f'<h1 style="color: rgb(9, 179, 23);">' 
    email_body += f'<b>Newly Added Tickers</b>: ' 
    email_body += f'{new}</h1>' 

    #Email Generation
    server = smtplib.SMTP(email_smtp_server,email_smtp_port) 
    print(f"Logging in to {email_sender_account}")
    server.starttls() 
    server.login(email_sender_username, email_sender_password)
    for recipient in email_recepients:
        print(f"Sending email to {recipient}")
        message = MIMEMultipart('alternative') 
        message['From'] = email_sender_account 
        message['To'] = recipient 
        message['Subject'] = email_subject 
        message.attach(MIMEText(email_body, 'html')) 
        server.sendmail(email_sender_account,recipient,message.as_string())
    server.quit()
            

if __name__ == '__main__':
    main()