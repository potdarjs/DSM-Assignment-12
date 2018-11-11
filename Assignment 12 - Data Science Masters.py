"""
Assignment 12
Data Science Masters
"""
#%%
#Read the following data set:
#https://archive.ics.uci.edu/ml/machine-learning-databases/adult/
#Rename the columns as per the description from this file:
#https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.names
#%%
import sqlite3
import pandas as pd
import numpy as np

url= "https://archive.ics.uci.edu/ml/machine-learning-databases/adult"
df = pd.read_csv(url+"//adult.data", header=None) 
df.head()
#%%
df.columns=["age","workclass","fnlwgt","education","education_num","marital_status","occupation","relationship","race","sex","capital_gain","capital_loss","hours_per_week","native_country","income_class"]
df.head()
#%%
#Task:
# Create a sql db from adult dataset and name it sqladb

from sqlalchemy import create_engine
engine = create_engine("sqlite:///sqladb", echo= False)
df.to_sql("sqladb", engine, if_exists='replace')
#%%
# Make a basic connection to the db
conn  = sqlite3.connect("sqladb")
cur  = conn.cursor()
#%%
# 1. Select 10 records from the adult sqladb

q1_df = pd.read_sql_query('SELECT * FROM sqladb limit 10;', conn)
q1_df.drop("index",axis=1,inplace=True)
q1_df
#%%
# 2. Show me the average hours per week of all men who are working in private sector

q2_df = pd.read_sql_query('SELECT avg(hours_per_week) as [Average hours per week] FROM sqladb WHERE workclass=" Private"', conn)
print(q2_df.columns[0],":",  q2_df["Average hours per week"][0]) 
q2_df
#%%
# 3. Show me the frequency table for education, occupation and relationship, separately

pd.read_sql_query("SELECT education as Education, COUNT(*) AS Frequency FROM sqladb GROUP BY education",conn)
#%%
pd.read_sql_query("SELECT occupation AS Occupation, COUNT(*) AS Frequency FROM sqladb GROUP BY occupation",conn)
#%%
pd.read_sql_query("SELECT relationship AS Relationship, COUNT(*) AS Frequency FROM sqladb GROUP BY relationship",conn)
#%%
# 4. Are there any people who are married, working in private sector and having a masters degree

q4_df = pd.read_sql_query('SELECT count(*) AS Count FROM sqladb WHERE education=" Masters" AND workclass=" Private" AND marital_status=" Married-civ-spouse"',conn) 
print(q4_df.columns[0], ":", q4_df["Count"][0])
print("Yes, there are people who are married, work in private sector and have a master's degree" if q4_df["Count"][0] > 0 else "No, there aren't any")

q4_df1 = pd.read_sql_query('SELECT * FROM sqladb WHERE education=" Masters" AND workclass=" Private" AND marital_status=" Married-civ-spouse"',conn) 
#q4_df1  will show an output which has 531 rows. Therefore, I'm only choosing to show 10 rows from the top
q4_df1.head(10)
#%%
# 5. What is the average, minimum and maximum age group for people working in different sectors

pd.read_sql_query("SELECT occupation as Occupation,avg(age) AS [Average Age], max(age) AS [Maximum Age], min(age) AS [Minimum Age] FROM sqladb GROUP BY occupation",conn)
#%%
# 6. Calculate age distribution by country 

print("\t\t Age distribution by country")
print("-"*65)
pd.read_sql_query("SELECT native_country as [Native Country],avg(age) AS [Average Age],max(age) AS [Maximum Age],min(age) AS [Minimum Age] FROM sqladb GROUP BY native_country",conn)
#%%
# 7. Compute a new column as 'Net-Capital-Gain' from the two columns 'capital-gain' and 'capital-loss'# 7. Com 

cur.execute('Alter table sqladb Add Net_Capital_Gain int')
cur.execute('UPDATE sqladb SET Net_Capital_Gain = capital_gain-capital_loss')
cur.close()
# This command will show an output which has 32561 rows. Therefore, I have chosen to output only 10 rows to show it has worked.
q7_df = pd.read_sql_query('SELECT * FROM sqladb limit 10', conn)
q7_df.drop("index",axis=1,inplace=True)
q7_df