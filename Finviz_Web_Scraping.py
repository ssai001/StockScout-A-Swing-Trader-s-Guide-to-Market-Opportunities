#Import required libraries
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date, datetime
from dotenv import load_dotenv
from IPython.display import HTML
from pretty_html_table import build_table
import bs4
import holidays
import json
import os
import pandas as pd
import requests
import smtplib
import sqlalchemy
import urllib



def main():
    
    #Define URL links to be used and aggregate into a list
    swingtrade1 = "https://finviz.com/screener.ashx?v=171&f=fa_epsqoq_pos,fa_salesqoq_pos,sh_curvol_o1000,ta_beta_o1,ta_highlow20d_b5h,ta_highlow52w_a70h,ta_sma20_sa50,ta_sma200_sb50,ta_sma50_pa&ft=4&o=-perf13w"
    swingtrade2 = "https://finviz.com/screener.ashx?v=171&f=sh_avgvol_o500,sh_price_u40,sh_relvol_o0.75,ta_pattern_tlsupport2&ft=4&o=-volume"
    swingtrade3 = "https://finviz.com/screener.ashx?v=171&f=sh_avgvol_o500,sh_float_u50,sh_outstanding_u50,sh_relvol_o2&ft=4&o=-volume"
    finviz_url_list = [swingtrade1,swingtrade2,swingtrade3]
    
    #Get list of stock market holidays and only run TickerDetection() and GenerateReport() functions if current trading day does not fall under a stock market holiday
    stock_market_holiday_list = [str(date[0]) for date in holidays.UnitedStates(years=datetime.now().year).items()]
    if datetime.today().strftime('%Y-%m-%d') not in stock_market_holiday_list:
        #DataRefresh()
        [TickerDetection(url) for url in finviz_url_list]
        GenerateReport()


def TickerDetection(request_url):
    
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
            try:
                appended_data_pd = appended_data_pd[['Ticker', 'SMA50', 'RSI', 'Price', 'Volume']]
                appended_data_pd = appended_data_pd.drop(appended_data_pd[ (appended_data_pd.Ticker == "-") | (appended_data_pd.SMA50 == "-") 
                | (appended_data_pd.RSI == "-") | (appended_data_pd.Price == "-") | (appended_data_pd.Volume == "-")].index)
            except KeyError:
                if i == 20:
                    print ("Exception: One or more of the URL links does not contain any records")
        
            
    #Append new columns including the following information: stock_name, reference url link, stock sector, and stock industry
    appended_data_pd["Name"] = [GetStockName(i) for i in appended_data_pd["Ticker"].tolist()]
    appended_data_pd["URL"] = [GetTickerWebsiteReference(i) for i in appended_data_pd["Ticker"].tolist()]
    appended_data_pd["Sector"] = [GetStockSector(i) for i in appended_data_pd["Ticker"].tolist()]
    appended_data_pd["Industry"] = [GetStockIndustry(i) for i in appended_data_pd["Ticker"].tolist()]
   
    
    # appended_data_pd['Ticker'] = appended_data_pd['Ticker'].apply(lambda x: f'<a href="https://finviz.com/quote.ashx?t={x}">{x}</a>')
    # HTML(appended_data_pd.to_html(escape=False))
    
    
    #Using sqlalchemy library, take appended_data_pd and upload to finviz_stock_screener table in PostgreSQL
    #All data is replaced in finviz_stock_screener during every run
    OpenPropertiesFile()
    connection = SQLEngine().raw_connection()
    cursor = connection.cursor()
    appended_data_pd.to_sql('finviz_stock_screener', SQLEngine(), if_exists='replace',
    dtype={'Ticker': sqlalchemy.VARCHAR(20), 
            'SMA50': sqlalchemy.types.VARCHAR(20), 
            'RSI':  sqlalchemy.types.DECIMAL, 
            'Price': sqlalchemy.types.DECIMAL,
            'Volume': sqlalchemy.types.BIGINT,
            'Name': sqlalchemy.VARCHAR(50),
            'URL': sqlalchemy.VARCHAR(50),
            'Sector': sqlalchemy.VARCHAR(50),
            'Industry': sqlalchemy.VARCHAR(50)})
    
    # #Call stored procedure finviz_all_list() in PostgreSQL which inserts the new tickers generated from above into a new table called finviz_all_list
    # #If there are any repeated records in finviz_stock_screener that occur in a subsequent run, finviz_all_list will update the existing ticker with new information from Finviz.com on that day
    cursor.execute("CALL finviz_all_list();")
    connection.commit()
    cursor.close()
    connection.close()
    SQLEngine().dispose()
    

def GetStockName(ticker):
    response = urllib.request.urlopen(f'https://query2.finance.yahoo.com/v1/finance/search?q={ticker}')
    content = response.read()
    data = json.loads(content.decode('utf8'))['quotes'][0]['shortname']
    return data


def GetTickerWebsiteReference(ticker):
    url_link = "https://finviz.com/quote.ashx?t={}".format(ticker)
    return url_link


def GetStockSector(ticker):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    page = requests.get("https://finviz.com/quote.ashx?t={}".format(ticker), headers= headers)
    soupysoupy = bs4.BeautifulSoup(page.text,'html.parser')
    tags = soupysoupy.find_all('a', {'class': 'tab-link', 'href': True})
    sector = str(tags[1]).split(">")[1][:-3]
    return sector


def GetStockIndustry(ticker):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0'}
    page = requests.get("https://finviz.com/quote.ashx?t={}".format(ticker), headers= headers)
    soupysoupy = bs4.BeautifulSoup(page.text,'html.parser')
    tags = soupysoupy.find_all('a', {'class': 'tab-link', 'href': True})
    industry = str(tags[2]).split(">")[1][:-3]
    return industry


def GenerateReport():  

    #Take final output from finviz_all_list table, enhance table look, and send as email
    OpenPropertiesFile()

    #Using data generated from finviz_all_list, filter important columns for both new and updated tickers which show change/difference in metrics
    finviz_report_updated = pd.read_sql_query('select "Count","Ticker","Name","URL","Sector","Industry","Initial_Insert","Last_Updated_On","SMA50_Behavior","RSI_Behavior","Price_Behavior","Volume_Behavior" from finviz_all_list where "Status" in (%(value1)s) order by "Count" desc, "Current_RSI" limit 25', 
    SQLEngine(), params = {"value1": 'UPDATED'})
    finviz_report_new_insert = pd.read_sql_query('select "Count","Ticker","Name","URL","Sector","Industry","Initial_Insert","Last_Updated_On","SMA50_Behavior","RSI_Behavior","Price_Behavior","Volume_Behavior" from finviz_all_list where "Status" in (%(value2)s) order by "Count" desc, "Current_RSI" limit 25', 
    SQLEngine(), params = {"value2": 'NEW INSERT'})

    #Enhance table look of finviz_report_updated and finviz_report_new_insert
    output1 = build_table(finviz_report_updated, 'blue_light')
    output2 = build_table(finviz_report_new_insert, 'blue_light')

    #Close sqlalchemy engine and call SendEmail() function to show outputted tables in email
    SQLEngine().dispose()
    SendEmail(output1,output2,datetime.today().strftime('%Y-%m-%d'))


def DataRefresh():
    
    #Initialize connection

    #Get the first weekday of the month. If its a holiday, then get the next weekday
    first_weekday_list = pd.date_range(date(date.today().year, 1, 1), date(date.today().year, 12, 31), freq='BMS')
    first_weekday_list = first_weekday_list.strftime("%Y-%m-%d").tolist()
    
    #Delete all data from finviz_stock_screener and finviz_all_list on first business day of the month 
    if datetime.today().strftime('%Y-%m-%d') in first_weekday_list:
        pd.read_sql_query('delete from finviz_all_list', SQLEngine())
        pd.read_sql_query('delete from finviz_stock_screener', SQLEngine())

    #Close sqlalchemy connection
    SQLEngine().dispose()
    pass


def SQLEngine():
    engine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(os.environ.get("DB_USER"),os.environ.get("DB_PWD"),os.environ.get("DB_HOST"),os.environ.get("DB_PORT"),os.environ.get("DB_NAME")))
    return engine

def OpenPropertiesFile():
    
    #Open app-config.env properties file and read environment variables
    load_dotenv(dotenv_path='app-config.env')
    

def SendEmail(input_list_1,input_list_2,date):
    
    #Initialize Properties File object
    OpenPropertiesFile()

    #Initialize Key Details
    email_sender_account = os.environ.get("email_sender_account")
    email_sender_username = os.environ.get("email_sender_username")
    email_sender_password = os.environ.get("email_sender_password")
    email_smtp_server = os.environ.get("email_smtp_server")
    email_smtp_port = os.environ.get("email_smtp_port")

    #Email Header
    email_recepients = os.environ.get("email_recepients").split(",")
    email_subject = f"Finviz Stock Tracker for {date}"
    email_body = '<html><head></head><body>' 
    
    #List of Updated Tickers
    email_body += f'<h1 style="color: rgb(86, 0, 251);">' 
    email_body += f'<b>Updated Tickers</b>: ' 
    email_body += f'{input_list_1}</h1>' 

    #List of New Tickers
    email_body += f'<h1 style="color: rgb(86, 0, 251);">' 
    email_body += f'<b>New Tickers</b>: ' 
    email_body += f'{input_list_2}</h1>'

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