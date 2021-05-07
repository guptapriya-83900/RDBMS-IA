#import csv files into mysql using pandas
import mysql.connector as mysql
from time import time
from mysql.connector import Error
import pandas as pd
data = pd.read_csv('C:/Users/Sandeep Jadhav/OneDrive/Desktop/ia2rdbms/netflix_titles.csv', index_col=False, delimiter = ',') # provide lakhrec csv file also here 
data = data.where((pd.notnull(data)), None) 
data=data.fillna(" ")   # if csv contains null it will convert into " " so no error 
data.head()
try:
    conn = mysql.connect(host='localhost', database='ia2', user='root', password= yourpassword)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE netflix(showid varchar(255),type varchar(255),title varchar(255),director varchar(255),cast varchar(1000),country varchar(255),datee varchar(255),releaseyr varchar(255),rating varchar(255),dur varchar(255),listedin varchar(255),des varchar(1000),primary key(showid))")
        print("Table is created....")
        #loop through the data frame   Series_reference,Period,Data_value,STATUS,UNITS,MAGNTUDE,Subject .. fro lakhrec csv file
        start=time()
        for i,row in data.iterrows():  #data.iterrows retuns dictionary
            sql = '''INSERT INTO ia2.netflix(showid,type,title,director,cast,country,datee,releaseyr,rating,dur,listedin,des) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
        end=time()
        print("Time taken for insertion :",end-start)  
except Error as e:
            print("Error while connecting to MySQL", e)

 