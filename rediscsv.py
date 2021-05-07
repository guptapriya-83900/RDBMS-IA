# import csv into redis (for both DBs)
import csv, redis, json
from time import time 
REDIS_HOST = 'localhost'

def read_csv_data(csv_file, ik, iv,it,iw):                # ik=0 iv=1 it=2 iw=3
    with open(csv_file, encoding='utf-8') as csvf:        # open csv file and return columns
        csv_data = csv.reader(csvf)
        return [(r[ik], r[iv],r[it],r[iw]) for r in csv_data]

def store_data(conn, data):
    for i in data:
        conn.setnx(i[0], i[1]+" "+i[2]+" "+i[3])  # setnx inserts multiple key value pairs in redis
    return data

def main():
    columns = (0,1,2,3)                                    # lakhrec -another csv file of 100000 records
    data = read_csv_data('netflix_titles.csv', *columns)   # 0 1 2 3 are the no of columns consideredd from csv file
    conn = redis.Redis(REDIS_HOST)    # redis connection
    strt=time()
    print (json.dumps(store_data(conn, data)))  #json.dumps writes in the form of key value pairs
    end=time()
    print("Time taken for insertion: ",end-strt)   # not query time but roughly gives idea about time 

if '__main__' == __name__:
    main()