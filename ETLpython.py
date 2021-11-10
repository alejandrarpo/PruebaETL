#import csv
import datetime
#import sys
#import uuid
import numpy 
import pandas as pd 
import pickle as pkl 

with open("ingr_map.pkl", "rb") as f:
    file= pkl.load(f)
    
from mysql import connector

def connection():

    config = {
        "user": "root",
        "password": "romin@123",
        "host": "localhost",
        "port": 3306,
        "charset" : 'utf8',
        "use_unicode" : True
    }
    try:
        c = connector.connect(**config)
        return c
    except:
        print("connection error")
        exit(1)


df_tmp = pd.DataFrame(file)

#df = pd.read_csv("interactions_validation.csv", encoding='utf-8', delimiter=',')
#print (df_tmp.head())

def extraccion():
    X = pd.read_csv("interactions_validation.csv", encoding='utf-8', delimiter=',')
    Y = pd.read_csv("interactions_test.csv", encoding='utf-8', delimiter=',')
    W = pd.read_csv("interactions_train.csv", encoding='utf-8', delimiter=',')
    A = pd.read_csv("PP_recipes.csv", encoding='utf-8', delimiter=',')
    B = pd.read_csv("RAW_recipes.csv", encoding='utf-8', delimiter=',')
    C = pd.read_csv("PP_users.csv", encoding='utf-8', delimiter=',')
    D = pd.read_csv("RAW_interactions.csv", encoding='utf-8', delimiter=',')
    return X,Y,W,A,B,C,D

def transform():
    X,Y,W,A,B,C,D= extraccion()
#print (X.head())

    X["date"]=pd.to_datetime(X["date"])
    return X

def load(dataclean):
    cur = dbconn.cursor()
    arraySize = len(dataclean)
    for r in range(0, arraySize):
        cur.execute('SET NAMES utf8mb4')
        cur.execute("SET CHARACTER SET utf8mb4")
        cur.execute("SET character_set_connection=utf8mb4")   
        cur.execute(
            """INSERT INTO (date,rating,recipe_id, user_id ) VALUES( %s,%s,%s,%s,)""",
                (dataclean[r][0], dataclean[r][1], dataclean[r][2], dataclean[r][3], dataclean[r][4]))
        dbconn.commit()
        
            

if __name__ == '__main__':

    print('realizando extracción')
    X,Y,W,A,B,C,D= extraccion()

    print('realizando transformación')
   
    '''
        mostrar base de datos existentes 
    '''
    show_db_query = "use prueba_alejandra"
    dbconn=connection()
    cur = dbconn.cursor()
    cur.execute(show_db_query)
    for x in cur:
        print(x)

    print('realizando carga de datos')
    load(Y)

   
