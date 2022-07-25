from requests_html import HTMLSession
from bs4 import BeautifulSoup
import requests 
import psycopg2


def get_range(limit,url_num):                     # get range from page numbers
    #find total no. of pages and set same to the numbers of urls.
    url = 'http://www.nepalstock.com/main/floorsheet/index/1/?contract-no=&stock-symbol=&buyer=&seller=&_limit='+ str(limit)
    r=requests.get(url)                 
    soup = BeautifulSoup(r.text,'html.parser') 
    page = soup.find('div', {'class':'pager'})

    pg = page.find('a').text
    pg=pg.split('/')
    if url_num == 9999:
        total_trans = int(pg[1])        
    else:
        total_trans = url_num            # reduce numbers for ease
    
    return total_trans


def get_urls(limit,url_num):          # create list of urls from given index and limit from nepalstock.com site
    url1  = 'http://www.nepalstock.com/main/floorsheet/index/'
    index = 1
    url3  = '/?contract-no=&stock-symbol=&buyer=&seller=&_limit='
    limit = limit
    url = url1+str(index)+url3+str(limit)
    urls=[]
    tot_index = int(get_range(limit,url_num))

    for i in range(1,tot_index+1):   
        url = url1+str(i)+url3+str(limit)
        urls.append(url)

    return urls


#GETS DATA INPUT DATE
def get_date():
    url = 'http://www.nepalstock.com/floorsheet'
    r=requests.get(url)
    soup = BeautifulSoup(r.text,'html.parser')
    date_row = soup.find('div', class_= 'col-xs-2 col-md-2 col-sm-0').text
    rest=date_row.split(' ')
    
    return rest[2]
    

#CONNECT TO POSTGRESQL
db_name = 'postgres'
db_user = 'postgres'
db_pass = 'password'
db_host = 'localhost'
db_port = '5432'
conn = psycopg2.connect(database = db_name, user = db_user, password = db_pass, host = db_host, port = db_port)
#print('database sucessfully connected')

# def delete_from_table(table_name):
#     cur= conn.cursor()
#     cur.execute(" DELETE FROM floorsheet_id WHERE date = '2022-02-10' ")
#     conn.commit()
#     print("rows deleted successfully :  ", table_name)

# delete_from_table("floorsheet_id")


def define_table():
    cur= conn.cursor()
    cur.execute(""" CREATE TABLE floorsheet_id ( ID SERIAL PRIMARY KEY, DATE TEXT, STOCK_SYMBOL TEXT , BUYER TEXT, 
                        SELLER TEXT , QUANTITY INT, RATE FLOAT, AMOUNT FLOAT )""")
    conn.commit()
    print("table created successfully")

#define_table()

#ENTER DATA INTO MAIN DATA STREAM
def save_data(limit,url_num,table_name):
    
    cur=conn.cursor()
    today = get_date() 

    for url in get_urls(limit,url_num):
    #for i in range(1,2):    
        
        r=requests.get(url)
        soup = BeautifulSoup(r.text,'html.parser')
        main_table = soup.find('table', { 'class' : 'table my-table'})
    
        data_table =main_table.find_all('tr')[2:-3]
        
        first_data = data_table[0].find_all('td')
        first_num=first_data[0].text               
        
        for data in data_table:

            data_single = data.find_all('td')
            
            num = data_single[0].text
            #Nolast = data_single[0].text
            Stock_Symbol = data_single[2].text
            Buyer = data_single[3].text
            Seller = data_single[4].text
            Quantity = int(data_single[5].text)
            Rate = float(data_single[6].text)
            Amount = float(data_single[7].text)
            print (Amount)
            if table_name == 'floorsheet_dummy':
                cur.execute("INSERT INTO FLOORSHEET_dummy(date, Stock_symbol, Buyer, Seller, Quantity, Rate,Amount) VALUES(%s,%s,%s,%s,%s,%s,%s)",(today,Stock_Symbol,Buyer,Seller,Quantity,Rate,Amount))
                conn.commit()

            if table_name == 'floorsheet_id':
                cur.execute("INSERT INTO floorsheet_id(date, Stock_symbol, Buyer, Seller, Quantity, Rate,Amount) VALUES(%s,%s,%s,%s,%s,%s,%s)",(today,Stock_Symbol,Buyer,Seller,Quantity,Rate,Amount))
                conn.commit()


        print('data added successfully from' ,first_num,'to',num )        
    conn.close()

table_name = 'floorsheet_id'    

save_data(10000,9999,table_name)          #      limit = no. of datas per pages, no. of pages ( 9999 for all transactions)

#save_data(limit,url_num,table_name)


 