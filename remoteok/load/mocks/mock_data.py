import pandas as pd
import random
import psycopg2
from sqlalchemy import create_engine

def connection():
    try:
        conn_string = ''
        db = create_engine(conn_string)
        conn = db.connect()
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        return conn
    except Exception as e:
        print('Error en conexion', e)
        return None


def read_file(route):
    try:
        return pd.read_csv(route)
    except:
        print('Error en la lectura de la datra')


def find_duplicates_positions(data):
    conn = connection()
    cursor = conn.cursor()
    sql1 = f'''SELECT *
    	       FROM position_category
               Where category = '{data}';
            '''
    cursor.execute(sql1)
    data = cursor.fetchall()
    conn.close()
    return data


def find_duplicates_companies(data):
    conn = connection()
    cursor = conn.cursor()
    sql1 = f'''SELECT *
    	       FROM Company
               Where Company = '{data}';
            '''
    cursor.execute(sql1)
    data = cursor.fetchall()
    conn.close()
    return data


def find_duplicates_offers(data):
    conn = connection()
    cursor = conn.cursor()
    sql1 = f'''
        SELECT * FROM jobOffer
        WHERE Position_id = (SELECT Position_id from Position where Position='{data["Position"]}' LIMIT 1) AND
              Company_id = (SELECT Company_id from Company WHERE Company ='{data["Company"]}' LIMIT 1) AND
              offer_location = '{data["Location"]}' AND
              date_published = '{data["Date_published"]}' AND
              offer_url = '{data["URL"]}' AND
              offer_description = '{data["Description"]}' AND
              Home_URL='{data["Home_URL"]}' AND
              Site_Name='{data["Site_Name"]}' AND
              MIN_SALARY={data["MIN_SALARY"]} AND
              MAX_SALARY={data["MAX_SALARY"]} AND
              MID_SALARY={data["MIDPOINT_SALARY"]} AND
              CURRENCY='{data["CURRENCY"]}' AND
              SALARY_PERIOD='{data["SALLARY_PERIOD"]}'
              LIMIT 1;
            '''
    cursor.execute(sql1)
    data = cursor.fetchall()
    conn.close()
    return data


def insert_position(data):
    print('INSERT INTO position_category(category) VALUES')
    data = data.drop_duplicates()
    for x in range(len(data)):
        position = data.iloc[x]
        # duplicates = find_duplicates_positions(position)
        # print(duplicates)
        print((f"('{position}'),"))
        
        # if not duplicates:
        #     conn = connection()
        #     cursor = conn.cursor()
        #     sql = f'''
        #     INSERT INTO position_category(category) VALUES ('{position}')
        #     '''
        #     cursor.execute(sql)
        #     conn.commit()
        #     conn.close()


def insert_company(data):
    print('INSERT INTO company(name,ceo) VALUES')
    data = data.drop_duplicates()
    for x in range(len(data)):
        position = data.iloc[x]
        print((f"('{position}','unknown'),"))
        # duplicates = find_duplicates_companies(position)
        # print(duplicates)
        # if not duplicates:
        #     conn = connection()
        #     cursor = conn.cursor()
        #     sql = f'''
        #     INSERT INTO company(name) VALUES ('{position}')
        #     '''
        #     cursor.execute(sql)
        #     conn.commit()
        #     conn.close()


def insert_dataa(data):
    print("""INSERT INTO position(position_title,
                                  position_category_id,
                                  seniority_id,
                                  modality,
                                  date_position,
                                  activate,
                                  num_offers,
                                  salary_min,
                                  salary_max,
                                  salary,
                                  currency_id,
                                  remote,
                                  description,
                                  location_id,
                                  english,
                                  english_level,
                                  position_url,
                                  company_id) values""")
    for row in range(len(data)):
        df = data.iloc[row]
        company = random.choice([2,63])
        category = random.choice([2,7])
        seniority = random.choice([1,4])
        currency = random.choice([1,2])
        location = random.choice([1,34])
        print(f"""('{df['Position']}',
                    {category},
                    {seniority},
                    'unknown',
                    '{df['Date_published']}',
                    '1',
                    3,
                    {df['MIN_SALARY']},
                    {df['MAX_SALARY']},
                    {df['MIDPOINT_SALARY']},
                    {currency},
                    '1',
                    '{df['Description']}',
                    {location},
                    '1',
                    'conversational',
                    '{df['URL']}',
                    {company}
                    ),""")



if __name__ == '__main__':
    # data = read_file(sys.argv[1])
    data = read_file('./clean_data_/REMOTEOK_2022-01-10_offers_CLEAN.csv')
    # insert_position(data["Position"])
    # insert_company(data['Company'])
    insert_dataa(data)
    

