import numpy as np
import psycopg2
from tabulate import tabulate
import plotly.graph_objects as go
import pandas as pd

#CONNECT TO POSTGRESQL
db_name = 'postgres'
db_user = 'postgres'
db_pass = 'password'
db_host = 'localhost'
db_port = '5432'
conn = psycopg2.connect(database = db_name, user = db_user, password = db_pass, host = db_host, port = db_port)
print('database sucessfully connected')
          

#TOP CONSEQUTIVE AMOUNT BOUGHT ---------------SINGLE COMPANY
def personal_top_buy(sdate,edate,company,num):
    cur= conn.cursor()
    select_Query = """with sorted as (
	SELECT * from FLOORSHEET_ID WHERE DATE >= %s AND DATE <= %s  
        order by stock_symbol,id)
    , ranges as ( 	SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
	    CASE WHEN lag(buyer) OVER () = buyer
	    THEN NULL ELSE 1 END r
	    FROM sorted  )
    , groups as (
	    SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
	    SUM (r) OVER (ORDER BY stock_symbol,id ) grp 
	    from ranges)

    ,final as ( SELECT  DATE, stock_symbol, buyer,grp, sum(amount) over (PARTITION BY grp ) CUML,
		   sum(quantity) over (PARTITION BY grp ) CUMQ ,count(*) over (partition by grp) Trnxs  from groups )
    ,top_list as(SELECT DISTINCT grp, DATE, stock_symbol, buyer, CUML,CUMQ,Trnxs FROM FINAL order by grp)

    ,CT as (SELECT DATE,STOCK_SYMBOL,BUYER,CUML,CUMQ,Trnxs FROM TOP_LIST WHERE STOCK_SYMBOL = %s ORDER BY CUML DESC LIMIT %s)
    
    SELECT CT.DATE, CT.STOCK_SYMBOL, CT.BUYER, CT.CUML, CT.CUMQ, CT.Trnxs,  PRICE_TODAY_ID.PC_CHANGE
    FROM CT, PRICE_TODAY_ID
    WHERE CT.DATE = PRICE_TODAY_ID.DATE AND CT.STOCK_SYMBOL = PRICE_TODAY_ID.STOCK_SYMBOL
    ORDER BY DATE, BUYER DESC, CUML DESC 
    """
    cur.execute(select_Query,( sdate,edate,company,num,))
    rows =cur.fetchall()
    #print (tabulate(rows , headers = ['grp','Company','Buyer_Broker','Amount','Quantity','Num_Trnxs'],tablefmt = 'psql',showindex = True))
    return rows

def ptb_details(sdate,edate,grp):                      #GET DETAILS FROM GRP .
    cur= conn.cursor()
    select_Query = """with sorted as (
        SELECT * from FLOORSHEET_ID where DATE >= %s AND DATE <= %s   order by stock_symbol,id)

    , ranges as ( 
        SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
        CASE WHEN lag(buyer) OVER () = buyer
        THEN NULL ELSE 1 END r
        FROM sorted  )
    , groups as (
        SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
        SUM (r) OVER (ORDER BY stock_symbol,id ) grp
        from ranges)
       
    SELECT * FROM groups where grp = %s
    """
    cur.execute(select_Query,(sdate,edate,grp,))
    rows =cur.fetchall()
    print (tabulate(rows , headers = ['id','date','Company','Buyer','Seller','Quantity','Rate','Amount','Null','Grp'],tablefmt = 'psql'))

#TOP CONSEQUTIVE AMOUNT SOLD
def personal_top_sell(sdate,edate,company,num):
    cur= conn.cursor()
    select_Query = """with sorted as (
	SELECT * from FLOORSHEET_ID where DATE >= %s AND DATE <= %s 
    order by stock_symbol,id)
    , ranges as ( 	SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
	    CASE WHEN lag(seller) OVER () = seller
	    THEN NULL ELSE 1 END r
	    FROM sorted  )
    , groups as (
	    SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
	    SUM (r) OVER (ORDER BY stock_symbol,id ) grp 
	    from ranges)

    ,final as ( SELECT  DATE, stock_symbol, seller,grp, sum(amount) over (PARTITION BY grp ) CUML,
		   sum(quantity) over (PARTITION BY grp ) CUMQ ,count(*) over (partition by grp) Trnxs  from groups )
    ,top_list as(SELECT DISTINCT grp,DATE, stock_symbol, seller, CUML,CUMQ,Trnxs FROM FINAL order by grp)

    ,CT as (SELECT DATE,STOCK_SYMBOL,SELLER,CUML,CUMQ,Trnxs FROM TOP_LIST WHERE STOCK_SYMBOL = %s ORDER BY CUML DESC LIMIT %s)
    
    SELECT CT.DATE, CT.STOCK_SYMBOL, CT.SELLER, CT.CUML, CT.CUMQ, CT.Trnxs,  PRICE_TODAY_ID.PC_CHANGE
    FROM CT, PRICE_TODAY_ID
    WHERE CT.DATE = PRICE_TODAY_ID.DATE AND CT.STOCK_SYMBOL = PRICE_TODAY_ID.STOCK_SYMBOL
    ORDER BY DATE, SELLER DESC, CUML DESC
    """  
    cur.execute(select_Query,(sdate,edate,company,num,))
    rows =cur.fetchall()
    #print (tabulate(rows , headers = ['grp','Company','Seller_Broker','Amount','Quantity','Num_Trnxs'],tablefmt = 'psql',showindex = True, stralign = ("left")))
    return rows

def pts_details(sdate,edate,grp):
    cur= conn.cursor()
    select_Query = """with sorted as (
        SELECT * from FLOORSHEET_ID where DATE >= %s AND DATE <= %s order by stock_symbol,id)

    , ranges as ( 
        SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
        CASE WHEN lag(seller) OVER () = seller
        THEN NULL ELSE 1 END r
        FROM sorted  )
    , groups as (
        SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
        SUM (r) OVER (ORDER BY stock_symbol,id ) grp
        from ranges)
       
    SELECT * FROM groups where grp = %s
    """
    cur.execute(select_Query,(sdate,edate,grp,))
    rows =cur.fetchall()
    print (tabulate(rows , headers = ['id','date','Company','Buyer','Seller','Quantity','Rate','Amount','Null','Grp'],tablefmt = 'psql'))


def cons_one(sdate,edate,company,num):
    top_buy = personal_top_buy(sdate,edate,company,num)
    # topper = 0
    # grp = top_buy[topper][0]
    # ptb_details(date,str(grp))

    top_sell = personal_top_sell(sdate,edate,company,num)
    # topper = 1
    # grp = top_sell[topper][0]
    # pts_details(date,str(grp))

    fuse = []
    for i in range(len(top_buy)):
        rate_buy = int(top_buy[i][3] / top_buy[i][4])
        rate_sell = int(top_sell[i][3] / top_sell[i][4])
        fuse.append( top_buy[i] + (rate_buy,)+ ('|||',)  + top_sell[i] +  (rate_sell,) )
    print( "\n DATE :",sdate , '--',edate,'----------SINGLE COMPANY----')
    print(tabulate(fuse,headers = ['DATE','Company','Buyer','Buy Amt','Qty', 'Notrn','Rate','pc_des','|',
               'DATE', 'grp','Seller','Sell Amt','Quantity', 'Notrn','Rate','pc_des'],tablefmt='pretty',showindex = True))

    df = pd.DataFrame(fuse)
    return df

#------------------------------------------------------------------------------------

#TOP CONSEQUTIVE AMOUNT BOUGHT ---------------ALL COMPANIES
def personal_top_buy1(sdate,edate,num):
    cur= conn.cursor()
    select_Query = """with sorted as (
	SELECT * from FLOORSHEET_ID WHERE DATE >= %s AND DATE <= %s  order by stock_symbol,id)
    , ranges as ( 	SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
	    CASE WHEN lag(buyer) OVER () = buyer
	    THEN NULL ELSE 1 END r
	    FROM sorted  )
    , groups as (
	    SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
	    SUM (r) OVER (ORDER BY stock_symbol,id ) grp 
	    from ranges)

    ,final as ( SELECT  DATE,stock_symbol, buyer,grp, sum(amount) over (PARTITION BY grp ) CUML,
		   sum(quantity) over (PARTITION BY grp ) CUMQ ,count(*) over (partition by grp) Trnxs  from groups )
    ,top_list as(SELECT DISTINCT grp,DATE, stock_symbol, buyer, CUML,CUMQ,Trnxs FROM FINAL order by grp)

    ,CT as (SELECT DATE,GRP,STOCK_SYMBOL,BUYER,CUML,CUMQ,Trnxs FROM TOP_LIST  ORDER BY CUML DESC LIMIT %s)
    
    SELECT CT.DATE, CT.STOCK_SYMBOL, CT.BUYER, CT.CUML, CT.CUMQ, CT.Trnxs,  PRICE_TODAY_ID.PC_CHANGE
    FROM CT, PRICE_TODAY_ID
    WHERE CT.DATE = PRICE_TODAY_ID.DATE AND CT.STOCK_SYMBOL = PRICE_TODAY_ID.STOCK_SYMBOL
    ORDER BY STOCK_SYMBOL, DATE, BUYER DESC, CUML DESC
    
     """
    
    cur.execute(select_Query,(sdate,edate,num,))
    rows =cur.fetchall()
    # print(tabulate(rows,tablefmt = 'psql',showindex = True))
    # print (tabulate(rows , headers = ['date','Company','Buyer_Broker','Amount','Quantity','Num_Trnxs','rate'],tablefmt = 'psql',showindex = True))
    return rows

#TOP CONSEQUTIVE AMOUNT SOLD -------ALL
def personal_top_sell1(sdate,edate,num):
    
    cur= conn.cursor()
    select_Query = """with sorted as (
	SELECT * from FLOORSHEET_ID where DATE >= %s AND DATE <= %s order by stock_symbol,id)
    , ranges as ( 	SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,
	    CASE WHEN lag(seller) OVER () = seller
	    THEN NULL ELSE 1 END r
	    FROM sorted  )
    , groups as (
	    SELECT id,date, stock_symbol, buyer, seller, quantity, rate, amount,r,
	    SUM (r) OVER (ORDER BY stock_symbol,id ) grp 
	    from ranges)

    ,final as ( SELECT  DATE,stock_symbol, seller,grp, sum(amount) over (PARTITION BY grp ) CUML,
		   sum(quantity) over (PARTITION BY grp ) CUMQ ,count(*) over (partition by grp) Trnxs  from groups )
    ,top_list as(SELECT DISTINCT grp,DATE, stock_symbol, seller, CUML,CUMQ,Trnxs FROM FINAL order by grp)

    ,CT as (SELECT DATE,GRP,STOCK_SYMBOL,seller,CUML,CUMQ,Trnxs FROM TOP_LIST ORDER BY CUML DESC LIMIT %s) 
    
    SELECT CT.DATE, CT.STOCK_SYMBOL, CT.seller, CT.CUML, CT.CUMQ, CT.Trnxs,  PRICE_TODAY_ID.PC_CHANGE
    FROM CT, PRICE_TODAY_ID
    WHERE CT.DATE = PRICE_TODAY_ID.DATE AND CT.STOCK_SYMBOL = PRICE_TODAY_ID.STOCK_SYMBOL
    ORDER BY STOCK_SYMBOL, DATE, SELLER DESC, CUML DESC
    """

    cur.execute(select_Query,(sdate,edate,num,))
    rows =cur.fetchall()
    # print (tabulate(rows , headers = ['Company','Seller_Broker','Amount','Quantity','Num_Trnxs','pc_des'],tablefmt = 'psql',showindex = True, stralign = ("left")))
    return rows


def cons_all(sdate,edate, num):
    top_buy = np.array(personal_top_buy1(sdate,edate,num)) 
    b= np.asarray(top_buy[:,3], dtype= np.float64 , order= 'C')
    c = np.asarray(top_buy[:,4], dtype= np.float64 , order= 'C')
    a = b/c
    a= np.around(a,1)
    a= a.reshape((len(a),1))
    top_buy= np.append(top_buy,a,axis=1)
        
    top_sell = np.array(personal_top_sell1(sdate,edate,num+1))
    sb= np.asarray(top_sell[:,3], dtype= np.float64 , order= 'C')
    sc= np.asarray(top_sell[:,4], dtype= np.float64 , order= 'C')
    sa= sb/sc
    sa= np.around(sa,1)
    sa= sa.reshape((len(sa),1))
    top_sell= np.append(top_sell,sa,axis=1)

    buncp,bunct = np.unique(top_buy[:,1],return_counts=True)
    suncp,sunct = np.unique(top_sell[:,1],return_counts=True)

    uncp = np.unique(np.append(buncp,suncp))

    # print(tabulate(matrix4,showindex=True))
    housing = np.full((0,16),'^')

    for item in uncp:
        bcp = bunct[np.isin(buncp,item)]
        scp = sunct[np.isin(suncp,item)]
                
        u_sell= top_sell[np.isin(top_sell[:,1], item)]
        u_buy = top_buy[np.isin(top_buy[:,1], item)]
        
        if bcp.size >0 and scp.size >0 :
            diff = bcp[0]-scp[0]
            if diff != 0:
                
                blank = np.full((abs(diff),8) ,'-')
                if diff > 0 :
                    u_sell = np.vstack((u_sell,blank)) 
                if diff < 0 :
                    u_buy = np.vstack((u_buy,blank)) 

        if bcp.size == 0:
            u_buy = np.full((scp[0],8) ,'-')
            
        if scp.size == 0:
            u_sell = np.full((bcp[0],8) ,'-')
     
        unit = np.append(u_buy,u_sell,axis=1)    
        # print(tabulate(unit,showindex=True,tablefmt='pretty'))
        wall = np.full((1,16),'======')
        room = np.append(unit,wall,axis=0)
        housing = np.append(housing,room,axis=0)


    print(tabulate(housing,headers = ['DATE','Company','Buyer','Buy Amt','Quantity', 'NoTrn','pc_de','Rate',
               'DATE','Company','Seller','Sell Amt','Quantity', 'Notrn','pc_de','Rate'],tablefmt='pretty',showindex = False))


    df = pd.DataFrame(housing)
    return df
#------------------------------------------------------------------------------------d

sdate =   '2022-07-24'  
edate =   '2022-07-24'  
company = 'LBBL'

period = sdate + "_" + edate


num =100
company_period = company +" "+ period + " top100" 

# one_comp = cons_one(sdate,edate,company,num)
# one_comp.to_excel(excel_writer = "C:/Users/nisha/Desktop/Trading/Data daily/"+ company_period + ".xlsx")


num = 800
allcomp_period = "all " +" "+ period +  " top1000" 

all_comp = cons_all(sdate,edate,num)
all_comp.to_excel(excel_writer = "C:/Users/nisha/Desktop/Trading/Data daily/"+ allcomp_period + ".xlsx")



conn.close()

