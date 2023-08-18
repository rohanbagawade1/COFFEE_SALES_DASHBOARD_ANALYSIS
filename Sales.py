#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import month
from pyspark.sql.functions import col, round


# In[2]:


spark = SparkSession.builder.appName('Sales').getOrCreate()


# In[3]:


Sales = spark.read.csv('C:/Users/lokes/OneDrive/Documents/Projects/sales.csv',inferSchema=True,header=True,encoding='utf8')


# In[5]:


Market = Sales['Area Code','Market','Market Size','State']
Market.show()


# In[6]:


Product = Sales['ProductID','Area Code','Product Type','Product','Type']
Product.show(100)


# In[38]:


Sales_data = Sales['ProductID','Date','Area Code','Profit','Margin','Sales','COGS','Total Expenses','Marketing','Inventory','Budget Profit','Budget COGS','Budget Margin','Budget Sales']
Sales_data.show(10)


# In[37]:


Sales_data.describe


# In[7]:


from pyspark.sql.types import TimestampType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import date_format
from pyspark.sql.functions import year


# In[8]:


calender = Sales.select('Date')


# In[9]:


cal = calender.withColumn('Date', to_timestamp(calender['Date'],'dd/MM/yy HH:mm:ss'))


# In[10]:


Calender = cal


# In[11]:


#Calender = Calender.select('r','Year')


# In[12]:


Calender = Calender.withColumn('Day',date_format(Calender['Date'],'E')).withColumn('Month', date_format(Calender['Date'], 'MMMM'))


# In[13]:


Calender = Calender.withColumn('Year',year(cal['Date']))


# In[14]:


Calender.show()


# In[15]:


#Sales_data.join(Product,on='ProductId',how='inner').select('ProductId','Profit','Product','Sales','Type').groupBy('ProductId').avg().show()


# # To sort the products by area code

# In[16]:


Product.orderBy("ProductID").show(200)


# <b>Find total expenses<b>

# In[17]:


exp=Sales_data.agg({'Total Expenses': 'sum'}).withColumnRenamed('sum(Total Expenses)', 'Total_Expenses')


# In[18]:


exp.show()


# <b>Find total profit<b>

# In[19]:


profit=Sales_data.agg({'Profit': 'sum'}).withColumnRenamed('sum(Profit)', 'Total_Profit')


# In[20]:


profit.show()


# <b>Avg Total sales<b>

# In[21]:


Avg=Sales_data.agg({'Sales':'average'}).withColumnRenamed('avg(Sales)', 'Avg_sales')


# In[22]:


Avg.show()


# <b>Average Sales,Profit,Expenses product wise<b>

# In[23]:


avg_pro_sales = Sales_data.join(Product, on='ProductId',how="inner").select('Product','Sales','Profit','Total Expenses').groupBy('Product').avg().show()


# <b>Product type wise profit<b>

# In[24]:


prod_sales = Sales_data.join(Product, on='ProductId',how="inner").select('Product Type','Profit').groupBy('Product Type').sum().show()


# In[25]:


pip install pymysql


# # Storing pyspark dataframe to SQL

# In[43]:


import pymysql
from sqlalchemy import create_engine


# In[9]:


#To establish connection to MySQL database  
connection = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='Lokesh@112',
    database='sales'
)


# In[10]:


cursor = connection.cursor()


# <b>STORING MARKET DATAFRAME INTO MYSQL<b>

# In[24]:


# Convert the Spark DataFrame to a Pandas DataFrame
pandas_df = Market.toPandas()


# In[25]:


# Define the CREATE TABLE query
market_table_query = """
    CREATE TABLE Market (
        Area_Code INT,
        Market VARCHAR(30),
        Market_Size VARCHAR(30),
        State VARCHAR(30)
    )
"""


# In[ ]:


# Execute the CREATE TABLE query
cursor.execute(market_table_query)


# In[70]:


#Inserting data into market table
insert_query = "INSERT INTO market (Area_Code, Market, Market_Size,State) VALUES (%s, %s, %s, %s)"
data = pandas_df.values.tolist()

#insert multiple rows of data into a table
cursor.executemany(insert_query, data)

# Commit the changes
connection.commit()


# <b>STORING PRODUCT DATAFRAME INTO MYSQL<b>

# In[125]:


pandas_df = Product.toPandas()


# In[126]:


product_table_query = """
    CREATE TABLE Product (
        ProductID INT,
        Area_Code INT,
        Product_Type VARCHAR(30),
        Product VARCHAR(30),
        Type VARCHAR(30)
    )
"""


# In[127]:


cursor.execute(product_table_query)


# In[ ]:


#Inserting data into Product table
insert_query = "INSERT INTO Product (ProductID, Area_Code, Product_Type, Product, Type) VALUES (%s, %s, %s, %s, %s)"
data = pandas_df.values.tolist()

#insert multiple rows of data into a table
cursor.executemany(insert_query, data)

# Commit the changes
connection.commit()


# <b>STORING SALES DATAFRAME TO MYSQL<b>

# In[39]:


pandas_df = Sales_data.toPandas()


# In[40]:


sales_table_query = """
    CREATE TABLE Sales (
      ProductID INT,
      `Date` DATE,
      Area_Code INT,
      Profit DOUBLE,
      Margin DOUBLE,
      Sales DOUBLE,
      COGS DOUBLE,
      TotalExpenses DOUBLE,
      Marketing DOUBLE,
      Inventory DOUBLE,
      BudgetProfit DOUBLE,
      BudgetCOGS DOUBLE,
      BudgetMargin DOUBLE,
      BudgetSales DOUBLE
)
"""


# In[41]:


cursor.execute(sales_table_query)


# In[42]:


insert_query = "INSERT INTO Sales (ProductID, Date, Area_Code, Profit, Margin, Sales, COGS, TotalExpenses, Marketing, Inventory, BudgetProfit, BudgetCOGS, BudgetMargin, BudgetSales) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
data = pandas_df.values.tolist()

# Insert multiple rows of data into a table
cursor.executemany(insert_query, data)

# Commit the changes
connection.commit()


# <b>STORING CALENDER DATAFRAME INTO MYSQL<b>

# In[49]:


pandas_df = Calender.toPandas()


# In[63]:


Calender_table_query = """
    CREATE TABLE Calender (
      Date TIMESTAMP,
      Day VARCHAR(255),
      Month VARCHAR(255),
      Year INT
)
"""


# In[64]:


cursor.execute(Calender_table_query)


# In[66]:


#Inserting data into Calender table
insert_query = "INSERT INTO Calender (Date, Day, Month, Year) VALUES (%s, %s, %s, %s)"
data = pandas_df.values.tolist()

#insert multiple rows of data into a table
cursor.executemany(insert_query, data)

# Commit the changes
connection.commit()


# In[ ]:


#Close the cursor
cursor.close()

#Close the MySQL connection
connection.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[6]:


Sales.show()


# In[ ]:





# In[ ]:




