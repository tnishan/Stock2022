from requests_html import HTMLSession
from bs4 import BeautifulSoup
import requests 
import psycopg2
from datetime import date
from tabulate import tabulate

#CONNECT TO POSTGRESQL
db_name = 'postgres'
db_user = 'postgres'
db_pass = 'password'
db_host = 'localhost'
db_port = '5432'
conn = psycopg2.connect(database = db_name, user = db_user, password = db_pass, host = db_host, port = db_port)
#print('database sucessfully connected')

user_agent_desktop = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '\
'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 '\
'Safari/537.36'

headers = { 'User-Agent': user_agent_desktop}

#DEFINE TABLE
def define_table():
    cur= conn.cursor()
    cur.execute(""" CREATE TABLE PRICE_TODAY_ID (ID SERIAL PRIMARY KEY ,DATE TEXT, STOCK_NAME TEXT, STOCK_SYMBOL TEXT , NUM_TRNX float, MAX_PRICE float,
                    MIN_PRICE float, CLOSING_PRICE float, TRADED_SHARES float, AMOUNT float, PREVIOUS_PRICE float,
                    DIFFERENCE float, PC_CHANGE float )""")
    conn.commit()
    print("table created successfully")
#define_table()


#TRUNCATE TABLE
def truncate_table():
    cur= conn.cursor()
    cur.execute(" TRUNCATE price_today_id;")
    conn.commit()
    print("table TRUNCATED successfully ")

#DELETE DATAS FROM TABLE TABLE
def delete_from_table(table_name):
    cur= conn.cursor()
    cur.execute(" DELETE FROM price_today_id WHERE date = '2021-11-17' ")
    conn.commit()
    print("table deleted successfully :  ", table_name)

# delete_from_table("price_today_id")

url = 'http://www.nepalstock.com/main/todays_price/index/4/?startDate=&stock-symbol=&_limit=300'


#GETS DATA INPUT DATE
def get_date():
    r=requests.get(url, headers = headers)
    soup = BeautifulSoup(r.text,'html.parser')
    date_row = soup.find('div', class_= 'col-xs-2 col-md-2 col-sm-0').text
    rest=date_row.split(' ')
    
    return rest[2]

def get_stock_symbol(stock_name):
    
    cur=conn.cursor()
    postgreSQL_select_Query = "select * from company_names where stock_name = %s"
    cur.execute(postgreSQL_select_Query, (stock_name,))
    top_buy =cur.fetchall()
    #print(top_buy)
    if top_buy ==[]:
        return '-'
    else:
        return top_buy[0][2]
   

#ENTER COMPANY NAMES INTO MAIN DATA STREAM
def save_data():
    
    cur=conn.cursor()
    r=requests.get(url,headers = headers)
    soup = BeautifulSoup(r.text,'html.parser')
  
    
    main_table = soup.find('table', { 'class' : 'table table-condensed table-hover'})
    data_table =main_table.find_all('tr')[2:-4]
    
    date_today = get_date()
    for data in data_table:

        data_single = data.find_all('td')
        
        id = int(data_single[0].text)
        
        stock_name = data_single[1].text
        stock_symbol = get_stock_symbol(stock_name)
        num_trnx = int(data_single[2].text)
        max_price = float(data_single[3].text)
        min_price = float(data_single[4].text)
        closing_price= float(data_single[5].text)
        traded_shares = float(data_single[6].text)
        amount = float(data_single[7].text)
        previous_price = float(data_single[8].text)
        difference = float(data_single[9].text)
        if previous_price ==0:
            pc_change = 0
        else:
            pc_change = float(str(round(difference/previous_price * 100,2)))

        cur.execute("INSERT INTO PRICE_TODAY_ID(date, STOCK_NAME, STOCK_SYMBOL, NUM_TRNX, MAX_PRICE, MIN_PRICE, CLOSING_PRICE, TRADED_SHARES, AMOUNT, PREVIOUS_PRICE, DIFFERENCE, PC_CHANGE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        ( date_today, stock_name, stock_symbol, num_trnx, max_price, min_price, closing_price,traded_shares, amount, previous_price, difference, pc_change))
        conn.commit()
        
    print('data added successfully in todays price table ')            
               
   
save_data()          #      limit = no. of datas per pages, no. of pages ( 9999 for all transactions)



