import pandas as pd
import psycopg2 as db

df = pd.read_csv('orders.csv')
#print(df.columns)

#rename columes and make them lower case and replace space with underscore
df.columns = df.columns.str.lower()
df.columns = df.columns.str.strip()
df.columns = df.columns.str.replace(' ', '_')

#derive new column discount, sale price and profit
df['discount'] = df['list_price']*df['discount_percent']*.01

#sale price
df['sale_price'] = df['list_price']-df['discount']

#profit
df['profit']= df['sale_price']-df['cost_price']

#dropping columns that are not important
df.drop(columns=['list_price', 'cost_price','discount_percent'], inplace=True)


#convert the data types of the columns to the right type
df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")
print(df.dtypes)
print(df.isnull().sum())
df.dropna(subset=['ship_mode'], inplace=True)
print(df.isnull().sum())


#connecting to postgres database
conn_string= "dbname='Orders_database' host='localhost' user='postgres' password='postgres'"
conn= db.connect(conn_string)
cur= conn.cursor()

#create table in postgres
cur.execute("""CREATE TABLE IF NOT EXISTS new_orders(
            order_id INT PRIMARY KEY,
            order_date DATE,
            ship_mode VARCHAR(255),
            segment VARCHAR(255),
            country VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255) ,
            postal_code VARCHAR(255), 
            region VARCHAR(255),
            category VARCHAR(255), 
            sub_category VARCHAR(255), 
            product_id VARCHAR(255), 
            quantity INT, 
            discount FLOAT, 
            sale_profit FLOAT, 
            profit FLOAT
)""")


#inserting data into our postgres server
query = "insert into new_orders (order_id, order_date,ship_mode, segment, country, city, state , postal_code, region, category,sub_category, product_id, quantity, discount, sale_profit, profit) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


#converting the dataframe into a list of tuples
data_tuples= [tuple(x) for x in df.to_numpy()]

#cur.mogrify(query,df.columns)
cur.executemany(query, data_tuples)
conn.commit()

cur.execute("SELECT product_id,SUM(sale_profit) as sales FROM public.new_orders GROUP BY product_id")
cur.execute("SELECT region,product_id,SUM(sale_profit) as sales FROM public.new_orders GROUP BY region,product_id ORDER BY region")


