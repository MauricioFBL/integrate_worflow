import pandas as pd
import random
import psycopg2
from sqlalchemy import create_engine
# import remoteok.load.conection as pc
import conection as pc


def read_file(route):
    try:
        return pd.read_csv(route)
    except:
        print('Error en la lectura de la datra')

# def find_duplicates_positions(data):
#     conn = connection()
#     cursor = conn.cursor()
#     sql1 = f'''SELECT *
#     	       FROM position_category
#                Where category = '{data}';
#             '''
#     cursor.execute(sql1)
#     data = cursor.fetchall()
#     conn.close()
#     return data


def get_data():
    conn = pc.connection_elephant()
    cursor = conn.cursor()
    sql1 = '''SELECT *
    	FROM "public"."position";'''
    sql1 = 'SELECT * FROM position;'
    cursor.execute(sql1)
    df = pd.read_sql_table('position', con=conn ,schema='public')
    # pd.DataFrame(c.fetchall())
    print(df)
    # for i in cursor.fetchall():
    #     print(i)

    
    cursor.close()
    conn.close()

get_data()