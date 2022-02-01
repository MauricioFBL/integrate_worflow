
import pandas as pd
from pipeline import main
import remoteok.load.conection as pc
import datetime as dt


def read_file(route):
    try:
        return pd.read_csv(route)
    except:
        print('Error en la lectura de la datra')


def find_duplicates_positions(data):
    conn = pc.connection()
    cursor = conn.cursor()
    sql1 = f'''SELECT *
    	       FROM Position
               Where Position = '{data}';
            '''
    cursor.execute(sql1)
    data = cursor.fetchall()
    conn.close()
    return data


def find_duplicates_companies(data):
    conn = pc.connection()
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
    conn = pc.connection()
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
    for x in range(len(data)):
        position = data.iloc[x]
        duplicates = find_duplicates_positions(position)
        print(duplicates)
        if not duplicates:
            conn = pc.connection()
            cursor = conn.cursor()
            sql = f'''
            INSERT INTO Position(Position) VALUES ('{position}')
            '''
            cursor.execute(sql)
            conn.commit()
            conn.close()


def insert_company(data):
    for x in range(len(data)):
        position = data.iloc[x]
        duplicates = find_duplicates_companies(position)
        print(duplicates)
        if not duplicates:
            conn = pc.connection()
            cursor = conn.cursor()
            sql = f'''
            INSERT INTO Company(Company) VALUES ('{position}')
            '''
            cursor.execute(sql)
            conn.commit()
            conn.close()


def insert_data(df):
    try:
        for x in range(len(df)):
            data = df.iloc[x]
            duplicates = find_duplicates_offers(data)
            print(duplicates)
            if not duplicates:
                conn = pc.connection()
                cursor = conn.cursor()
                sql = f'''
                CALL NEW_OFFER('{data["Position"]}', 
                               '{data["Company"]}',
                               '{data["Location"]}',
                               '{data["Date_published"]}',
                               '{data["URL"]}',
                               '{data["Description"]}',
                               '{data["Home_URL"]}',
                               '{data["Site_Name"]}',
                               {data["MIN_SALARY"]},
                               {data["MAX_SALARY"]},
                               {data["MIDPOINT_SALARY"]},
                               '{data["CURRENCY"]}',
                               '{data["SALLARY_PERIOD"]}');
                '''
                print(sql)
                cursor.execute(sql)
                conn.commit()
                conn.close()
        print('DATOS INSERTADOS CORRECTAMENTE')
    except Exception as e:
        print('ERROR EN LA INSERCION: ', e)


def get_data():
    conn = pc.connection()
    cursor = conn.cursor()
    sql1 = '''SELECT *
    	FROM "researchTests"."jobOffers";'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        print(i)


def main():
    # data = read_file(sys.argv[1])
    today = dt.date.today()
    data = read_file(f'.remoteok/clean_data/REMOTEOK_{today}_oyfers_CLEAN.csv')
    insert_position(data["Position"])
    insert_company(data['Company'])
    insert_data(data)
    # get_data()



if __name__ == '__main__':
    main()